package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.table.Record;

/**
 * QueryPlan provides a set of functions to generate simple queries. Calling the methods corresponding
 * to SQL syntax stores the information in the QueryPlan, and calling execute generates and executes
 * a QueryPlan DAG.
 */
public class QueryPlan {
  public enum PredicateOperator {
    EQUALS,
    NOT_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS
  }

  private Database.Transaction transaction;
  private QueryOperator finalOperator;
  private String startTableName;

  private List<String> joinTableNames;
  private List<String> joinLeftColumnNames;
  private List<String> joinRightColumnNames;
  private List<String> whereColumnNames;
  private List<PredicateOperator> whereOperators;
  private List<DataType> whereDataTypes;
  private List<String> selectColumns;
  private String groupByColumn;
  private boolean hasCount;
  private String averageColumnName;
  private String sumColumnName;



  /**
   * Creates a new QueryPlan within transaction. The base table is startTableName.
   *
   * @param transaction the transaction containing this query
   * @param startTableName the source table for this query
   */
  public QueryPlan(Database.Transaction transaction, String startTableName) {
    this.transaction = transaction;
    this.startTableName = startTableName;

    this.selectColumns = new ArrayList<String>();
    this.joinTableNames = new ArrayList<String>();
    this.joinLeftColumnNames = new ArrayList<String>();
    this.joinRightColumnNames = new ArrayList<String>();

    this.whereColumnNames = new ArrayList<String>();
    this.whereOperators = new ArrayList<PredicateOperator>();
    this.whereDataTypes = new ArrayList<DataType>();

    this.hasCount = false;
    this.averageColumnName = null;
    this.sumColumnName = null;

    this.groupByColumn = null;

    this.finalOperator = null;
  }

  public QueryOperator getFinalOperator() {
    return this.finalOperator;
  }

  /**
   * Add a select operator to the QueryPlan with a list of column names. Can only specify one set
   * of selections.
   *
   * @param columnNames the columns to select
   * @throws QueryPlanException
   */
  public void select(List<String> columnNames) throws QueryPlanException {
    if (!this.selectColumns.isEmpty()) {
      throw new QueryPlanException("Cannot add more than one select operator to this query.");
    }

    if (columnNames.isEmpty()) {
      throw new QueryPlanException("Cannot select no columns.");
    }

    this.selectColumns = columnNames;
  }

  /**
   * Add a where operator. Only returns columns in which the column fulfills the predicate relative
   * to value.
   *
   * @param column the column to specify the predicate on
   * @param comparison the comparator
   * @param value the value to compare against
   * @throws QueryPlanException
   */
  public void where(String column, PredicateOperator comparison, DataType value) throws QueryPlanException {
    this.whereColumnNames.add(column);
    this.whereOperators.add(comparison);
    this.whereDataTypes.add(value);
  }

  /**
   * Set the group by column for this query.
   *
   * @param column the column to group by
   * @throws QueryPlanException
   */
  public void groupBy(String column) throws QueryPlanException {
    this.groupByColumn = column;
  }

  /**
   * Add a count aggregate to this query. Only can specify count(*).
   *
   * @throws QueryPlanException
   */
  public void count() throws QueryPlanException {
    this.hasCount = true;
  }

  /**
   * Add an average on column. Can only average over integer or float columns.
   *
   * @param column the column to average
   * @throws QueryPlanException
   */
  public void average(String column) throws QueryPlanException {
    this.averageColumnName = column;
  }

  /**
   * Add a sum on column. Can only sum integer or float columns
   *
   * @param column the column to sum
   * @throws QueryPlanException
   */
  public void sum(String column) throws QueryPlanException {
    this.sumColumnName = column;
  }

  /**
   * Join the leftColumnName column of the existing queryplan against the rightColumnName column
   * of tableName.
   *
   * @param tableName the table to join against
   * @param leftColumnName the join column in the existing QueryPlan
   * @param rightColumnName the join column in tableName
   */
  public void join(String tableName, String leftColumnName, String rightColumnName) {
    this.joinTableNames.add(tableName);
    this.joinLeftColumnNames.add(leftColumnName);
    this.joinRightColumnNames.add(rightColumnName);
  }

  /**
   * Generates a naïve QueryPlan in which all joins are at the bottom of the DAG followed by all where
   * predicates, an optional group by operator, and a set of selects (in that order).
   *
   * @return an iterator of records that is the result of this query
   * @throws DatabaseException
   * @throws QueryPlanException
   */
  public Iterator<Record> execute() throws DatabaseException, QueryPlanException {
    String indexColumn = this.checkIndexEligible();

    if (indexColumn != null) {
      this.generateIndexPlan(indexColumn);
    } else {
      // start off with the start table scan as the source
      this.finalOperator = new SequentialScanOperator(this.transaction, this.startTableName);

      this.addJoins();
      this.addWheres();
      this.addGroupBy();
      this.addSelects();
    }

    return this.finalOperator.execute();
  }

  /**
   * Generates an optimal QueryPlan based on the System R cost-based query optimizer.
   *
   * @return an iterator of records that is the result of this query
   * @throws DatabaseException
   * @throws QueryPlanException
   */
  public Iterator<Record> executeOptimal() throws DatabaseException, QueryPlanException {
    List<String> tableNames = new ArrayList<String>();
    tableNames.add(this.startTableName);
    tableNames.addAll(this.joinTableNames);
    int pass = 1;

    // Pass 1: Iterate through all single tables. For each single table, find
    // the lowest cost QueryOperator to access that table. Construct a mapping
    // of each table name to its lowest cost operator.
    Map<Set, QueryOperator> map = new HashMap<Set, QueryOperator>();
    for (String table : tableNames) {
      QueryOperator minOp = this.minCostSingleAccess(table);
      Set<String> key = new HashSet<String>();
      key.add(table);
      map.put(key, minOp);
    }

    // Pass i: On each pass, use the results from the previous pass to find the
    // lowest cost joins with each single table. Repeat until all tables have
    // been joined.
    Map<Set, QueryOperator> pass1Map = map;
    Map<Set, QueryOperator> prevMap;
    while (pass++ < tableNames.size()) {
      prevMap = map;
      map = this.minCostJoins(prevMap, pass1Map);
    }

    // Get the lowest cost operator from the last pass, add GROUP BY and SELECT
    // operators, and return an iterator on the final operator
    this.finalOperator = this.minCostOperator(map);
    this.addGroupBy();
    this.addSelects();
    return this.finalOperator.iterator();
  }

  /**
   * Gets all WHERE predicates for which there exists an index on the column
   * referenced in that predicate for the given table.
   *
   * @return an ArrayList of WHERE predicates
   */
  private List<Integer> getEligibleIndexColumns(String table) {
    List<Integer> whereIndices = new ArrayList<Integer>();

    for (int i = 0; i < this.whereColumnNames.size(); i++) {
      String column = this.whereColumnNames.get(i);
      if (this.transaction.indexExists(table, column) &&
          this.whereOperators.get(i) != PredicateOperator.NOT_EQUALS) {
        whereIndices.add(i);
      }
    }

    return whereIndices;
  }

  /**
   * Applies all eligible WHERE predicates to a given source, except for the
   * predicate at index except. The purpose of except is because there might
   * be one WHERE predicate that was already used for an index scan, so no
   * point applying it again. A WHERE predicate is represented as elements of
   * this.whereColumnNames, this.whereOperators, and this.whereDataTypes that
   * correspond to the same index of these lists.
   *
   * @return a new QueryOperator after WHERE has been applied
   * @throws DatabaseException
   * @throws QueryPlanException
   */
  private QueryOperator pushDownWheres(QueryOperator source, int except) throws QueryPlanException, DatabaseException {
    // TODO: implement me!
//    QueryOperator toRtn = source;
    for (int i = 0; i < this.whereColumnNames.size(); i++) {
        if (i != except) {
            try {
                source = new WhereOperator(source, whereColumnNames.get(i), whereOperators.get(i), whereDataTypes.get(i));
//                toRtn = new WhereOperator(toRtn, whereColumnNames.get(i), whereOperators.get(i), whereDataTypes.get(i));
            } catch (QueryPlanException q) {
                break;
            }
        }
    }
    return source;
//    return toRtn;
  }

  /**
   * Finds the lowest cost QueryOperator that scans the given table. First
   * determine the cost of a sequential scan. Then for every index that can be
   * used on that table, determine the cost of an index scan. Keep track of
   * the minimum cost operation. Then push down eligible selects (WHERE
   * predicates). If an index scan was chosen, exclude that WHERE predicate from
   * the push down. This method is called during the first pass of the search
   * algorithm to determine the most efficient way to access each single table.
   *
   * @return a QueryOperator that scans the given table
   * @throws DatabaseException
   * @throws QueryPlanException
   */
  private QueryOperator minCostSingleAccess(String table) throws DatabaseException, QueryPlanException {
    QueryOperator minOp = null;

    // Find the cost of a sequential scan of the table
    // TODO: implement me!

    QueryOperator scanOp = (new SequentialScanOperator(this.transaction, table));
    int lowestCost = scanOp.estimateIOCost();
    minOp = scanOp;

    // For each eligible index column, find the cost of an index scan of the
    // table and retain the lowest cost operator
    List<Integer> whereIndices = this.getEligibleIndexColumns(table);
    int minWhereIdx = -1;
    // TODO: implement me!

    for (Integer i: whereIndices) {
        PredicateOperator op = whereOperators.get(i);
        QueryOperator qop = (new IndexScanOperator(this.transaction, table, whereColumnNames.get(i), op, whereDataTypes.get(i)));
        int cost = qop.estimateIOCost();
        if (cost < lowestCost) {
            lowestCost = cost;
            minOp = qop;
            minWhereIdx = i;
        }
    }

    // Push down WHERE predicates that apply to this table and that were not
    // used for an index scan
    minOp = this.pushDownWheres(minOp, minWhereIdx);
    return minOp;
  }

  /**
   * Given a join condition between an outer relation represented by leftOp
   * and an inner relation represented by rightOp, find the lowest cost join
   * operator out of all the possible join types in JoinOperator.JoinType.
   *
   * @return lowest cost join QueryOperator between the input operators
   * @throws QueryPlanException
   */
  private QueryOperator minCostJoinType(QueryOperator leftOp,
                                        QueryOperator rightOp,
                                        String leftColumn,
                                        String rightColumn,
                                        boolean reverse) throws QueryPlanException,
                                                                   DatabaseException {
    QueryOperator minOp = null;

    // TODO: implement me!
    QueryOperator op = new SNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
    QueryOperator revOp;
    minOp = op;
    int lowestCost = minOp.estimateIOCost();
    int cost;
    int revCost;
    if (reverse) {
        revOp = new SNLJOperator(rightOp, leftOp, rightColumn, leftColumn, this.transaction);
        revCost = revOp.estimateIOCost();
        if (revCost < lowestCost) {
            lowestCost = revCost;
            minOp = revOp;
        }
    }
    for (int i = 1; i<3; i++) {
        if (i == 0) {
            op = new PNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
            cost = op.estimateIOCost();
            if (reverse) {
                revOp = new PNLJOperator(rightOp, leftOp, rightColumn, leftColumn, this.transaction);
                revCost = revOp.estimateIOCost();
                if (revCost < cost) {
                    op = revOp;
                    cost = revCost;
                }
            }
        }
        else if (i == 1) {
            op = new BNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
            cost = op.estimateIOCost();
            if (reverse) {
                revOp = new BNLJOperator(rightOp, leftOp, rightColumn, leftColumn, this.transaction);
                revCost = revOp.estimateIOCost();
                if (revCost < cost) {
                    op = revOp;
                    cost = revCost;
                }
            }
        } else {
            op = new GraceHashOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction);
            cost = op.estimateIOCost();
            if (reverse) {
                revOp = new GraceHashOperator(rightOp, leftOp, rightColumn, leftColumn, this.transaction);
                revCost = revOp.estimateIOCost();
                if (revCost < cost) {
                    op = revOp;
                    cost = revCost;
                }
            }
        }
        if (cost < lowestCost) {
            minOp = op;
            lowestCost = cost;
        }
    }

    return minOp;
  }

  /**
   * Iterate through all table sets in the previous pass of the search. For each
   * table set, check each join predicate to see if there is a valid join
   * condition with a new table. If so, check the cost of each type of join and
   * keep the minimum cost join. Construct and return a mapping of each set of
   * table names being joined to its lowest cost join operator. A join predicate
   * is represented as elements of this.joinTableNames, this.joinLeftColumnNames,
   * and this.joinRightColumnNames that correspond to the same index of these lists.
   *
   * @return a mapping of table names to a join QueryOperator
   * @throws QueryPlanException
   */
  private Map<Set, QueryOperator> minCostJoins(Map<Set, QueryOperator> prevMap,
                                               Map<Set, QueryOperator> pass1Map) throws QueryPlanException,
                                                                                        DatabaseException {
    Map<Set, QueryOperator> map = new HashMap<Set, QueryOperator>();

    // TODO: implement me!
//    System.out.println(joinTableNames);
    for (Map.Entry<Set, QueryOperator> entry: prevMap.entrySet()) {
        Set set = entry.getKey();
//        Set set2;
//        set2.addAll()
//        System.out.println(set);
        QueryOperator op = entry.getValue();
        boolean reverse = (set.size() == 1);
        for (Map.Entry<Set, QueryOperator> pass1Entry: pass1Map.entrySet()) {
            String name = (String) pass1Entry.getKey().iterator().next();
            QueryOperator op1 = pass1Entry.getValue();
//            boolean valid = false;
            if (name.equals(joinTableNames.get(0)) && !set.contains(name)) {
                String leftCol = joinLeftColumnNames.get(0);
                String rightCol = joinRightColumnNames.get(0);
                QueryOperator qop = minCostJoinType(op, op1, leftCol, rightCol, reverse);
                set.add(name);
                map.put(set, qop);
//                valid = true;
            }
//            if (joinTableNames.size() > 1) {
//                System.out.println("here we go");
//            }
//            if (joinTableNames.contains(name) && !set.contains(name)) {
//            if (valid) {
////                int index = joinTableNames.indexOf(name);
//                String leftCol = joinLeftColumnNames.get(0);
//                String rightCol = joinRightColumnNames.get(0);
//                QueryOperator qop = minCostJoinType(op, op1, leftCol, rightCol, reverse);
//                set.add(name);
//                map.put(set, qop);
//            }
        }
    }

    return map;
  }

  /**
   * Finds the lowest cost QueryOperator in the given mapping. A mapping is
   * generated on each pass of the search algorithm, and relates a set of tables
   * to the lowest cost QueryOperator accessing those tables. This method is
   * called at the end of the search algorithm after all passes have been
   * processed.
   *
   * @return a QueryOperator in the given mapping
   * @throws QueryPlanException
   */
  private QueryOperator minCostOperator(Map<Set, QueryOperator> map) throws QueryPlanException, DatabaseException {
    QueryOperator minOp = null;
    QueryOperator newOp;
    int minCost = Integer.MAX_VALUE;
    int newCost;
    for (Set tables : map.keySet()) {
      newOp = map.get(tables);
      newCost = newOp.getIOCost();
      if (newCost < minCost) {
        minOp = newOp;
        minCost = newCost;
      }
    }
    return minOp;
  }

  private String checkIndexEligible() {
    if (this.whereColumnNames.size() > 0
        && this.groupByColumn == null
        && this.joinTableNames.size() == 0) {

      int index = 0;
      for (String column : whereColumnNames) {
        if (this.transaction.indexExists(this.startTableName, column)) {
          if (this.whereOperators.get(index) != PredicateOperator.NOT_EQUALS) {
            return column;
          }
        }

        index++;
      }
    }

    return null;
  }

  private void generateIndexPlan(String indexColumn) throws QueryPlanException, DatabaseException {
    int whereIndex = this.whereColumnNames.indexOf(indexColumn);
    PredicateOperator operator = this.whereOperators.get(whereIndex);
    DataType value = this.whereDataTypes.get(whereIndex);

    this.finalOperator = new IndexScanOperator(this.transaction, this.startTableName, indexColumn, operator,
        value);

    this.whereColumnNames.remove(whereIndex);
    this.whereOperators.remove(whereIndex);
    this.whereDataTypes.remove(whereIndex);

    this.addWheres();
    this.addSelects();
  }

  private void addJoins() throws QueryPlanException, DatabaseException {
    int index = 0;

    for (String joinTable : this.joinTableNames) {
      SequentialScanOperator scanOperator = new SequentialScanOperator(this.transaction, joinTable);

      SNLJOperator joinOperator = new SNLJOperator(finalOperator, scanOperator,
          this.joinLeftColumnNames.get(index), this.joinRightColumnNames.get(index), this.transaction); //changed from new JoinOperator

      this.finalOperator = joinOperator;
      index++;
    }
  }

  private void addWheres() throws QueryPlanException, DatabaseException {
    int index = 0;

    for (String whereColumn : this.whereColumnNames) {
      PredicateOperator operator = this.whereOperators.get(index);
      DataType value = this.whereDataTypes.get(index);

      WhereOperator whereOperator = new WhereOperator(this.finalOperator, whereColumn,
          operator, value);

      this.finalOperator = whereOperator;
      index++;
    }
  }

  private void addGroupBy() throws QueryPlanException, DatabaseException {
    if (this.groupByColumn != null) {
      if (this.selectColumns.size() > 2 || (this.selectColumns.size() == 1 &&
          !this.selectColumns.get(0).equals(this.groupByColumn))) {
        throw new QueryPlanException("Can only select columns specified in the GROUP BY clause.");
      }

      GroupByOperator groupByOperator = new GroupByOperator(this.finalOperator, this.transaction,
          this.groupByColumn);

      this.finalOperator = groupByOperator;
    }
  }

  private void addSelects() throws QueryPlanException, DatabaseException {
    if (!this.selectColumns.isEmpty() || this.hasCount || this.sumColumnName != null
        || this.averageColumnName != null) {
      SelectOperator selectOperator = new SelectOperator(this.finalOperator, this.selectColumns,
          this.hasCount, this.averageColumnName, this.sumColumnName);

      this.finalOperator = selectOperator;
    }
  }
}

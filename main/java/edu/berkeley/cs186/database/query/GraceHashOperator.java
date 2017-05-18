package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
//import edu.berkeley.cs186.database.table.Schema;
//import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;
//import org.relaxng.datatype.Datatype;


public class GraceHashOperator extends JoinOperator {

  private int numBuffers;

  public GraceHashOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      Database.Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource,
          rightSource,
          leftColumnName,
          rightColumnName,
          transaction,
          JoinType.GRACEHASH);

    this.numBuffers = transaction.getNumMemoryPages();
    this.stats = this.estimateStats();
    this.cost = this.estimateIOCost();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new GraceHashIterator();
  }

  public int estimateIOCost() throws QueryPlanException {
    // TODO: implement me!
      int numRPages = getLeftSource().getStats().getNumPages();
      int numSPages = getRightSource().getStats().getNumPages();
      return 3*(numRPages+numSPages);
  }

  /**
   * An implementation of Iterator that provides an iterator interface for this operator.
   */
  private class GraceHashIterator implements Iterator<Record> {
    private Iterator<Record> leftIterator;
    private Iterator<Record> rightIterator;
    private Record rightRecord;
    private Record nextRecord;
    private String[] leftPartitions;
    private String[] rightPartitions;
    private int currentPartition;
    private Map<DataType, ArrayList<Record>> inMemoryHashTable;

    private boolean isInt;
    private boolean isBool;
    private boolean isFloat;
    private boolean isString;
    private int currentIndex;
    private String currRightPart;

    private int leftIndex = getLeftColumnIndex();
    private int rightIndex = getRightColumnIndex();
    Database.Transaction transaction;
    private Record leftRecord;
    private ArrayList<Record> leftRecords;
    private DataType rightType = null;
    private List<DataType> rightValues = null;
//    DataType realType;


    public GraceHashIterator() throws QueryPlanException, DatabaseException {
      this.leftIterator = getLeftSource().iterator();
      this.rightIterator = getRightSource().iterator();
      leftPartitions = new String[numBuffers - 1];
      rightPartitions = new String[numBuffers - 1];
      String leftTableName;
      String rightTableName;
      for (int i = 0; i < numBuffers - 1; i++) {
        leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
        rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
        GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
        GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
        leftPartitions[i] = leftTableName;
        rightPartitions[i] = rightTableName;
      }

      // TODO: implement me!
      int modValue;
      transaction = getTransaction();
      leftRecords = new ArrayList<>();

      //first iterate through left records to put them in partitions
      modValue = leftPartitions.length;
      while (leftIterator.hasNext()) {
          Record rec = leftIterator.next();
          int recHash = rec.getValues().get(leftIndex).hashCode() % modValue;
          String tableIn = leftPartitions[recHash];
          transaction.addRecord(tableIn, rec.getValues());
      }

      //now, iterate through right records to put them in partitions
      modValue = rightPartitions.length;
      while (rightIterator.hasNext()) {
          Record rec = rightIterator.next();
          int recHash = rec.getValues().get(rightIndex).hashCode() % modValue;
          String tableIn = rightPartitions[recHash];
          transaction.addRecord(tableIn, rec.getValues());
      }

      //now, put LEFT partitions into a hashmap, which... i'm unsure how to even use
      inMemoryHashTable = new HashMap<>();
      for (String partition: leftPartitions) {
          ArrayList<Record> recs;
          Iterator<Record> iter = transaction.getRecordIterator(partition);
          while (iter.hasNext()) {
              Record rec = iter.next();
              DataType realType = rec.getValues().get(leftIndex);
              if (inMemoryHashTable.containsKey(realType)) {
                  inMemoryHashTable.get(realType).add(rec);
//                  recs = inMemoryHashTable.get(realType);
              } else {
                  recs = new ArrayList<>();
                  recs.add(rec);
                  inMemoryHashTable.put(realType, recs);
              }
//              System.out.println(realType);
//              recs.add(rec);
//              inMemoryHashTable.put(realType, recs);
          }
//          inMemoryHashTable.put(type, recs);
      }

    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      // TODO: implement me!
          if (nextRecord != null) {
              return true;
          }
      while (true) {
//          System.out.println("another iter");
          while (rightRecord == null) {
              if (rightIterator.hasNext()) {
                  rightRecord = rightIterator.next();

                  rightType = rightRecord.getValues().get(rightIndex);
                  rightValues = new ArrayList<>(rightRecord.getValues());
                  leftRecords = new ArrayList<>(inMemoryHashTable.get(rightType));

                  break;
              } else {
                  if (currentPartition == rightPartitions.length) {
                      return false;
                  }
                  try {
                      rightIterator = transaction.getRecordIterator(rightPartitions[currentPartition]);
                  } catch (DatabaseException d) {
                      return false;
                  }
                  currentPartition++;
              }
          }
          this.leftRecord = leftRecords.remove(0);

          DataType leftJoinValue = this.leftRecord.getValues().get(leftIndex);

          if (leftJoinValue.equals(rightType)) {
              List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());

              leftValues.addAll(rightValues);
              this.nextRecord = new Record(leftValues);
              if (leftRecords.isEmpty()) {
                  this.rightRecord = null;
              }
              return true;
          }
          if (leftRecords.isEmpty()) {
              System.out.println("entered this check");
              this.rightRecord = null;
          }

      }
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      // TODO: implement me!
        if (hasNext()) {
            Record r = nextRecord;
            this.nextRecord = null;
            return r;
        }
        throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

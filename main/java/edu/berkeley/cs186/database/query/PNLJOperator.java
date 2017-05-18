package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordID;
//import edu.berkeley.cs186.database.table.Schema;
//import edu.berkeley.cs186.database.table.Table;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class PNLJOperator extends JoinOperator {

    public PNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.PNLJ);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new PNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        // TODO: implement me!
        int numRPages = getLeftSource().getStats().getNumPages();
        int numSPages = getRightSource().getStats().getNumPages();
//        String RName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
//        int numRRecsPerPage = 0;
//        try {
//            numRRecsPerPage = getNumEntriesPerPage(RName);
//        } catch (DatabaseException e) {
//            e.printStackTrace();
//        }
//        String SName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
//        int numSRecsPerPage = 0;
//        try {
//            numSRecsPerPage = getNumEntriesPerPage(SName);
//        } catch (DatabaseException e) {
//            e.printStackTrace();
//        }
//
        return (numRPages)*numSPages+numRPages;
//        return -1;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class PNLJIterator implements Iterator<Record> {
        private String leftTableName;
        private String rightTableName;
        private Iterator<Page> leftIterator;
        private Iterator<Page> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private Page leftPage;
        private Page rightPage;
        private Queue<Record> leftRecords;
        private Queue<Record> rightRecords;
        boolean leftPageReset;


        public PNLJIterator() throws QueryPlanException, DatabaseException {
            if (PNLJOperator.this.getLeftSource().isSequentialScan()) {
                this.leftTableName = ((SequentialScanOperator) PNLJOperator.this.getLeftSource()).getTableName();
            } else {
                this.leftTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getLeftColumnName() + "Left";
                PNLJOperator.this.createTempTable(PNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
                Iterator<Record> leftIter = PNLJOperator.this.getLeftSource().iterator();
                while (leftIter.hasNext()) {
                    PNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
                }
            }

            if (PNLJOperator.this.getRightSource().isSequentialScan()) {
                this.rightTableName = ((SequentialScanOperator) PNLJOperator.this.getRightSource()).getTableName();
            } else {
                this.rightTableName = "Temp" + PNLJOperator.this.getJoinType().toString() + "Operator" + PNLJOperator.this.getRightColumnName() + "Right";
                PNLJOperator.this.createTempTable(PNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
                Iterator<Record> rightIter = PNLJOperator.this.getRightSource().iterator();
                while (rightIter.hasNext()) {
                    PNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
                }
            }

            // TODO: implement me!
            leftIterator = PNLJOperator.this.getPageIterator(leftTableName);
            rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
            leftRecord = null;
            rightRecord = null;
            nextRecord = null;
            leftPage = null;
            rightPage = null;
            leftRecords = null;
            rightRecords = null;
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            //TODO: implement me!
            //beautiful, fast code
            if (this.nextRecord != null) {
                return true;
            }

            while (true) {
                if (leftPage == null) {
                    if (leftIterator.hasNext()) {
                        leftPage = leftIterator.next();
                    } else {
                        return false;
                    }
                } else {

                    if (rightPage == null) {  //will be null when we're done going through right pages, when leftpage is changed
                        if (!rightIterator.hasNext()) { //if it doesn't have a next... that means we're done, set the top to null and recurse again
                            if (leftPageReset) {
                                try {
                                    rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
                                } catch (DatabaseException d) {
                                    return false;
                                }
                                leftPageReset = false;
                            } else {
                                leftPageReset = true;
                                leftPage = null;
                            }
                        } else {
                            rightPage = rightIterator.next();
                            if (rightPage.getPageNum() == 0) {
                                if (rightIterator.hasNext()) {
                                    rightPage = rightIterator.next();
                                } else {
                                    rightPage = null;
                                }
                            }
                        }
                    } else {
                        if (leftRecord == null) {
                            if (leftRecords == null) {
                                try {
                                    leftRecords = getRecords(leftTableName, leftPage.getPageNum());
                                    leftRecord = leftRecords.poll();
                                } catch (DatabaseException d) {
                                    return false;
                                }
                            } else if (leftRecords.isEmpty()) {
                                rightPage = null;
                                leftRecords = null;
                            } else {
                                leftRecord = leftRecords.poll();
                            }
                        } else {
                            if (rightRecord == null) {
                                if (rightRecords == null) {
                                    try {
                                        rightRecords = getRecords(rightTableName, rightPage.getPageNum());
                                    } catch (DatabaseException d) {
                                        return false;
                                    }
                                } else if (rightRecords.isEmpty()) {
                                    leftRecord = null;
                                    rightRecords = null;
                                }
                            }

                            while (rightRecords != null && !rightRecords.isEmpty()) {
                                rightRecord = rightRecords.poll();
                                leftPageReset = false;
                                DataType leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                                DataType rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());

                                if (leftJoinValue.equals(rightJoinValue)) {
                                    List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                                    List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());

                                    leftValues.addAll(rightValues);
                                    this.nextRecord = new Record(leftValues);
                                    this.rightRecord = null;
                                    return true;
                                }
                            }
                            this.rightRecord = null;
                        }
                    }

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

         /**
          * Gives all valid records on a given page, via a Queue
          * @return the Queue of valid records
          * @throws DatabaseException if it is not possible to get the record
          */
        public Queue<Record> getRecords(String name, int pageNum) throws DatabaseException {
            Queue<Record> toRtn = new LinkedList<>();
            int entryCount = 0;
            try {
                entryCount = PNLJOperator.this.getTransaction().getNumEntriesPerPage(name);
            } catch (DatabaseException d) {
                throw new DatabaseException("invalid table");
            }
            for (int i = 0; i < entryCount; i++) { //add all entries in page
                RecordID rid = new RecordID(pageNum, i);
                Record rec;
                try {
                    rec = PNLJOperator.this.getTransaction().getRecord(name, rid);
                } catch (DatabaseException d) {
                    continue;
                }
                toRtn.add(rec);
            }

            return toRtn;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.table.stats.TableStats;

public class BNLJOperator extends JoinOperator {

    private int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getNumMemoryPages();
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new BNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        // TODO: implement me!
        int numRPages = getLeftSource().getStats().getNumPages();
        int numSPages = getRightSource().getStats().getNumPages();
        return (int)(Math.ceil((float)numRPages/(BNLJOperator.this.numBuffers-2))*numSPages+numRPages);
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class BNLJIterator implements Iterator<Record> {
        private String leftTableName;
        private String rightTableName;
        private Iterator<Page> leftIterator;
        private Iterator<Page> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private Page leftPage;
        private Page rightPage;
        private Page[] block;
        private ArrayList<Page> pageBlock;
        private Queue<Record> leftRecords;
        private Queue<Record> rightRecords;
        boolean leftPageReset;
        final private int size = BNLJOperator.this.numBuffers-2;


        public BNLJIterator() throws QueryPlanException, DatabaseException {
            if (BNLJOperator.this.getLeftSource().isSequentialScan()) {
                this.leftTableName = ((SequentialScanOperator)BNLJOperator.this.getLeftSource()).getTableName();
            } else {
                this.leftTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getLeftColumnName() + "Left";
                BNLJOperator.this.createTempTable(BNLJOperator.this.getLeftSource().getOutputSchema(), leftTableName);
                Iterator<Record> leftIter = BNLJOperator.this.getLeftSource().iterator();
                while (leftIter.hasNext()) {
                    BNLJOperator.this.addRecord(leftTableName, leftIter.next().getValues());
                }
            }
            if (BNLJOperator.this.getRightSource().isSequentialScan()) {
                this.rightTableName = ((SequentialScanOperator)BNLJOperator.this.getRightSource()).getTableName();
            } else {
                this.rightTableName = "Temp" + BNLJOperator.this.getJoinType().toString() + "Operator" + BNLJOperator.this.getRightColumnName() + "Right";
                BNLJOperator.this.createTempTable(BNLJOperator.this.getRightSource().getOutputSchema(), rightTableName);
                Iterator<Record> rightIter = BNLJOperator.this.getRightSource().iterator();
                while (rightIter.hasNext()) {
                    BNLJOperator.this.addRecord(rightTableName, rightIter.next().getValues());
                }
            }

            // TODO: implement me!
            leftIterator = BNLJOperator.this.getPageIterator(leftTableName);
            rightIterator = BNLJOperator.this.getPageIterator(rightTableName);
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
                if (pageBlock == null) {
                    if (leftIterator.hasNext()) {
                        pageBlock = nextBlock();
                    } else {
                        return false;
                    }
                } else {

                    if (rightPage == null) {  //will be null when we're done going through right pages, when leftpage is changed
                        if (!rightIterator.hasNext()) { //if it doesn't have a next... that means we're done, set the top to null and recurse again
                            if (leftPageReset) {
                                try {
                                    rightIterator = BNLJOperator.this.getPageIterator(rightTableName);
                                } catch (DatabaseException d) {
                                    return false;
                                }
                                leftPageReset = false;
                            } else {
                                leftPageReset = true;
                                pageBlock = null;
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
                                    leftRecords = getBlockRecords(leftTableName, pageBlock);
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
                                DataType leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                                DataType rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());

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


        public Queue<Record> getBlockRecords(String name, ArrayList<Page> pages) throws DatabaseException {
            Queue<Record> toRtn = new LinkedList<>();
            int entryCount = 0;
            try {
                entryCount = BNLJOperator.this.getTransaction().getNumEntriesPerPage(name);
            } catch (DatabaseException d) {
                throw new DatabaseException("invalid table");
            }
            Record rec;
            for (int j = 0; j < pages.size(); j++) {
                for (int i = 0; i < entryCount; i++) { //add all entries in page
                    RecordID rid = new RecordID(pages.get(j).getPageNum(), i);
                    try {
                        rec = BNLJOperator.this.getTransaction().getRecord(name, rid);
                    } catch (DatabaseException d) {
                        continue;
                    }
                    toRtn.add(rec);
                }
            }
            return toRtn;
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
                entryCount = BNLJOperator.this.getTransaction().getNumEntriesPerPage(name);
            } catch (DatabaseException d) {
                throw new DatabaseException("invalid table");
            }
            for (int i = 0; i < entryCount; i++) { //add all entries in page
                RecordID rid = new RecordID(pageNum, i);
                Record rec;
                try {
                    rec = BNLJOperator.this.getTransaction().getRecord(name, rid);
                } catch (DatabaseException d) {
                    continue;
                }
                toRtn.add(rec);
            }

            return toRtn;
        }

        /**
         *
         * @return the queue of pages in the block
         */
        public ArrayList<Page> nextBlock() {
            ArrayList<Page> toRtn = new ArrayList<>();
            int counter = 0;
            while (leftIterator.hasNext() && counter < size) {
                Page thisPage = leftIterator.next();
                if (thisPage.getPageNum() == 0) {
                    if (leftIterator.hasNext()) {
                        thisPage = leftIterator.next();
                    }
                }
                toRtn.add(thisPage);
                counter++;
            }
            return toRtn;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

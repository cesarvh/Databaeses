package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.table.Record;
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
//        int iOCost = 0;
//        int rightNumPages = BNLJOperator.this.getRightSource().getStats().getNumPages();
//        int leftNumPages = BNLJOperator.this.getLeftSource().getStats().getNumPages();
//
//        iOCost = ((leftNumPages / (BNLJOperator.this.numBuffers - 2)) * rightNumPages) + leftNumPages;
//        return iOCost;

        return (int) ((Math.ceil( (double) BNLJOperator.this.getLeftSource().getStats().getNumPages()/ (double) (BNLJOperator.this.numBuffers - 2))
                * (double) BNLJOperator.this.getRightSource().getStats().getNumPages()) + BNLJOperator.this.getLeftSource().getStats().getNumPages());
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

        private int numBuffersAvail;
        private int currLeftNum;
        private int currRightNum;
        private int bufferPointer;
        private  int bufferFill;

        Boolean restart;

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

            // set the block, iterators and buffer counts up
            this.numBuffersAvail = BNLJOperator.this.numBuffers - 2;
            this.leftIterator = BNLJOperator.this.getPageIterator(leftTableName);
            this.rightIterator = BNLJOperator.this.getPageIterator(rightTableName);

            // Set up the initial pointers
            this.currLeftNum = 0;
            this.currRightNum = 0;
            this.bufferPointer = 0;
            this.bufferFill = 0;

            this.block = new Page[this.numBuffersAvail];
            Page p;
            int i = 0;
            while (i < this.block.length) {
                if (this.leftIterator.hasNext()) {
                    p = this.leftIterator.next();
                    if (p.getPageNum() != 0) {
                        this.block[i] = p;
                        this.bufferFill++;
                        i++;
                    }
                } else {
                    break;
                }
            }

            // get the intial pages
            this.leftPage = this.block[this.bufferPointer];

            this.rightPage = this.rightIterator.next();
            this.rightPage = this.rightIterator.next();

            this.leftRecord = retrieveNextLeftRecord();

            this.restart = false;

        }


        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         * I'm assuming you've already done PNLJ. The logic is nearly the same, but instead of moving to the next page of S any time you
         * run out of records in your current page of R, you should get the next page of R from the block, and move to the next of S whenever
         * you finish the block. (At that point you also need to reset the block, and move the block at the end of S).
         *
         *
         */
        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            try {

                while (true) {

                    this.rightRecord = retrieveNextRightRecord();
                    this.restart = false;
                    mainLeft:
                    while (this.leftRecord != null) {
                        while (this.rightRecord != null) {

                            DataType leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                            DataType rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                            if (leftJoinValue.equals(rightJoinValue)) {
                                List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                                List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
                                leftValues.addAll(rightValues);
                                this.nextRecord = new Record(leftValues);

                                return true;
                            } else {
                                this.restart = true;
                                break mainLeft;
                            }
                        }
                        this.leftRecord = retrieveNextLeftRecord();
                        this.currRightNum = 0;
                        this.rightRecord = retrieveNextRightRecord();

                    }
                    if (!restart) {

                        if (this.rightIterator.hasNext()) {
                            // continue on the next page to the right
                            this.rightPage = this.rightIterator.next();
                            this.currRightNum = 0;
                            this.currLeftNum = 0;
                            this.bufferPointer = 0;
                            this.leftRecord = this.retrieveNextLeftRecord();
                        } else {
                            if (this.currLeftNum + 1 < BNLJOperator.this.getNumEntriesPerPage(leftTableName)) {
                                this.currLeftNum++;
                                this.currRightNum = 0;
                            } else if (this.leftIterator.hasNext()) {
//                                if (this.leftIterator.hasNext()) {
                                refillBuffer();
                                this.rightIterator = BNLJOperator.this.getPageIterator(this.rightTableName);
                                this.rightPage = this.rightIterator.next();
                                this.rightPage = this.rightIterator.next(); // skip 0th page
                                this.currLeftNum = 0;
                                this.currRightNum = 0;
                                this.bufferPointer = 0;
//                                this.leftPage = this.block[this.bufferPointer];
//                                this.bufferPointer++;
                                this.leftRecord = retrieveNextLeftRecord();

                            } else {
                                return false;
                            }
                        }

                    } else {
                        continue;
                    }
                }

                } catch (Exception e) {
                return false;
            }
        }


        private void refillBuffer() {
            this.block = new Page[this.numBuffersAvail];
            Page p;
            int i = 0;
            this.bufferPointer = 0;
            while (i < this.block.length) {
                if (this.leftIterator.hasNext()) {
                    p = this.leftIterator.next();
                    this.block[i] = p;
                    this.bufferFill++;
                    i++;
//                    }
                } else {
                    break;
                }
            }
            this.leftPage = this.block[this.bufferPointer];

        }

        private Record retrieveNextLeftRecord() {
            try {
                while (this.bufferPointer < this.numBuffersAvail) {
                    if (this.bufferPointer < this.bufferFill) {
                        this.leftPage = this.block[this.bufferPointer];
                    } else {
                        return null;
                    }

                    while (this.currLeftNum < BNLJOperator.this.getNumEntriesPerPage(leftTableName)) {
                        byte[] leftHeader = BNLJOperator.this.getPageHeader(rightTableName, this.leftPage);
                        byte b = leftHeader[this.currLeftNum / 8];
                        int bitOffset = 7 - (this.currLeftNum % 8);
                        byte mask = (byte) (1 << bitOffset);
                        byte value = (byte) (b & mask);
                        if (value != 0) {
                            int entrySize = BNLJOperator.this.getLeftSource().getOutputSchema().getEntrySize();
                            int offset = BNLJOperator.this.getHeaderSize((leftTableName)) + (entrySize * currLeftNum);
                            byte[] bytes = this.leftPage.readBytes(offset, entrySize);
                            Record toRtn = BNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
                            this.currLeftNum++;
                            return toRtn;

                        }
                        this.currLeftNum++;
                    }

                    this.bufferPointer++;
                    this.currLeftNum = 0;
                }

            } catch (Exception e) {
                return null;
            }
            return null;
        }
        private Record retrieveNextRightRecord() {
            try{
                while (true) {
                    while (this.currRightNum < BNLJOperator.this.getNumEntriesPerPage(rightTableName)) {
                        if (rightPage.getPageNum() == 0) { this.rightPage = this.rightIterator.next(); }
                        byte[] rightHeader = BNLJOperator.this.getPageHeader(rightTableName,this.rightPage);
                        byte b = rightHeader[this.currRightNum / 8];
                        int bitOffset = 7 - (this.currRightNum % 8);
                        byte mask = (byte) (1 << bitOffset);
                        byte value = (byte) (b & mask);
                        if (value != 0) {
                            int entrySize = BNLJOperator.this.getRightSource().getOutputSchema().getEntrySize();
                            int offset = BNLJOperator.this.getHeaderSize((rightTableName)) + (entrySize * currRightNum);
                            byte[] bytes = this.rightPage.readBytes(offset, entrySize);
                            Record toRtn = BNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
                            this.currRightNum++;
                            return toRtn;

                        }
                        this.currRightNum++;
                    }
                    return null;
                }
            } catch (Exception e){
                return null;
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
            if (this.hasNext()) {
                Record r = this.nextRecord;
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
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
import edu.berkeley.cs186.database.table.Table;
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
        // numEntriesPerPage = BNLJOperator.this.getNumEntriesPerPage(tableName);
        //  numEntriesPerTable = BNLJOperator.this.getLeftSource().getStats().getNumRecords();

        int leftpages = PNLJOperator.this.getLeftSource().getStats().getNumRecords();
        int rightpages = PNLJOperator.this.getRightSource().getStats().getNumRecords();
        return rightpages*leftpages + leftpages;
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
        //I created these variables
        private int currLeftNum;
        private int currRightNum;

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
            //
            this.leftIterator = PNLJOperator.this.getPageIterator(leftTableName);
            this.leftPage = this.leftIterator.next();
            this.rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
            this.rightPage = this.rightIterator.next();

            this.currLeftNum = 0;
            this.currRightNum = 0;
        }

        public boolean hasNext() {
            if (this.nextRecord != null){
                return true;
            }

            while (this.leftPage != null) {
                try {
                    if (retrieveNextRightRecord() == null){
                        if (this.currLeftNum + 1 < PNLJOperator.this.getNumEntriesPerPage(leftTableName)){
                            this.currLeftNum++;
                            this.currRightNum = 0;
                        } else {

                            if (this.rightIterator.hasNext()) {

                                this.rightPage = this.rightIterator.next();
                                this.currRightNum = 0;
                                this.currLeftNum = 0;
                            } else {

                                this.leftPage = this.leftIterator.next();
                                this.currLeftNum = 0;
                                this.rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
                                this.rightPage = this.rightIterator.next();
                                this.currRightNum = 0;
                            }
                        }
                    }
                    if (retrieveNextLeftRecord() == null){

                        if (this.rightIterator.hasNext()){
                            this.rightPage = this.rightIterator.next();
                            this.currLeftNum = 0;
                            this.currRightNum = 0;
                        } else{

                            if (this.leftIterator.hasNext()){
                                this.leftPage = this.leftIterator.next();
                                this.currLeftNum = 0;
                                this.currRightNum = 0;
                                this.rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
                                this.rightPage = this.rightIterator.next();
                            }
                        }
                    }
                    this.leftRecord = retrieveNextLeftRecord();
                    this.rightRecord = retrieveNextRightRecord();
                    this.currRightNum++;

                    if (this.rightRecord == null) {
                        continue;
                    }
                    
                    DataType leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                    DataType rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {

                        List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                        List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                        return true;
                    }
                } catch (Exception e){
                    return false;
                }
            }
            return false;
        }

        private Record retrieveNextLeftRecord() {
            try{
                while(true) {
                    while (this.currLeftNum < PNLJOperator.this.getNumEntriesPerPage(leftTableName)) {
                        if (leftPage.getPageNum() == 0) { this.leftPage = this.leftIterator.next(); }

                        byte[] leftHeader = PNLJOperator.this.getPageHeader(rightTableName,this.leftPage);
                        byte b = leftHeader[this.currLeftNum/8];
                        int bitOffset = 7 - (this.currLeftNum % 8);
                        byte mask = (byte) (1 << bitOffset);
                        byte value = (byte) (b & mask);
                        if (value != 0) {
                            int entrySize = PNLJOperator.this.getEntrySize(leftTableName);
                            int offset = PNLJOperator.this.getHeaderSize((leftTableName)) + (entrySize * currLeftNum);
                            byte[] bytes = this.leftPage.readBytes(offset, entrySize);
                            Record toRtn = PNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
                            return toRtn;
                        }
                        this.currLeftNum++;
                    }
                    return null;
                }
            } catch (Exception e){
                return null;
            }
        }
        private Record retrieveNextRightRecord() {
            try{
                while (true) {
                    while (this.currRightNum < PNLJOperator.this.getNumEntriesPerPage(rightTableName)) {
                        if (rightPage.getPageNum() == 0) { this.rightPage = this.rightIterator.next(); }
                        byte[] rightHeader = PNLJOperator.this.getPageHeader(rightTableName,this.rightPage);
                        byte b = rightHeader[this.currRightNum / 8];
                        int bitOffset = 7 - (this.currRightNum % 8);
                        byte mask = (byte) (1 << bitOffset);
                        byte value = (byte) (b & mask);
                        if (value != 0) {
                            int entrySize = PNLJOperator.this.getEntrySize(rightTableName);
                            int offset = PNLJOperator.this.getHeaderSize((rightTableName)) + (entrySize * currRightNum);
                            byte[] bytes = this.rightPage.readBytes(offset, entrySize);
                            Record toRtn = PNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
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

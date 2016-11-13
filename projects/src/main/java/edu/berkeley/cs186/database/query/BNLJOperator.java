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
    return -1;
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
        this.numBuffersAvail = BNLJOperator.this.numBuffers; // - 2;
        this.leftIterator = BNLJOperator.this.getPageIterator(leftTableName);
        this.rightIterator = BNLJOperator.this.getPageIterator(rightTableName);
        this.leftPage = this.leftIterator.next();
        this.rightPage = this.rightIterator.next();
        this.block = new Page[numBuffersAvail];
        this.block[0] = this.leftPage;
        Page p;
        for (int i = 1; i < this.numBuffersAvail; i++) {

            if (this.leftIterator.hasNext()) {
                p = this.leftIterator.next();
            } else {
                break;
            }
            this.block[i] = p;
        }
//        this.leftPage = block[0];

        this.currLeftNum = 0;
        this.currRightNum = 0;
        this.bufferPointer = 0;

      // TODO: implement me!

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
      // TODO: implement me!
        if (this.nextRecord != null) {
            return true;
        }
        while (this.leftPage != null) {
            try {
                this.rightRecord = retrieveNextRightRecord();
                this.leftRecord = retrieveNextLeftRecord();

                if (this.rightRecord == null) {
                    if (this.leftRecord == null) {
                        if (this.bufferPointer == this.block.length - 1) {
                            if (this.rightIterator.hasNext()) {
                                this.rightPage = this.rightIterator.next();
                                this.bufferPointer = 0;
                                this.currLeftNum = 0;
                                this.currRightNum = 0;


                            }
                        } else {
                            bufferPointer++;
                            this.leftPage = block[bufferPointer];
                            this.currRightNum = 0;
                            this.currLeftNum = 0;


                        }
                    } else {
                        this.currRightNum = 0;
                        this.currLeftNum+= 1;
                    }
                }

                // now the stuff thats meant to happen
                this.leftRecord = retrieveNextLeftRecord();
                this.rightRecord = retrieveNextRightRecord();
                this.currRightNum++;


                // Case 1
                if (this.leftRecord == null) {
                    if (this.currLeftNum + 1 < BNLJOperator.this.getNumEntriesPerPage(leftTableName)) {
                        this.currLeftNum++;
                        this.currRightNum = 0;
                    }
                }
                // Case 2
                if (this.leftRecord == null) {
                    if (this.bufferPointer < this.block.length) {
                        this.leftPage = this.block[bufferPointer];
                        this.bufferPointer++;
                        this.currLeftNum = 0;
                        this.currRightNum = 0; // dont switch the page, but do switch the LEFT page :)
                    }
                }
                // case 3
                if (this.leftRecord == null) {
                    if (this.bufferPointer == this.block.length - 1) {
                        if (this.rightIterator.hasNext()) {
                            this.bufferPointer = 0;
                            this.currRightNum = 0;
                            this.currLeftNum = 0;
                            this.rightPage = this.rightIterator.next();
                        }

                    }
                }
                // case 4
                if (this.leftRecord == null) {
                    if (this.bufferPointer == this.block.length - 1) { // nothing else on block
                        if (!this.rightIterator.hasNext()) {
                            this.leftPage = this.leftIterator.next();
                            for (int i = 0; i < this.numBuffersAvail; i++) {
                                this.block[i] = this.leftPage;
                                if (this.leftIterator.hasNext()) {
                                    this.leftPage = this.leftIterator.next();
                                } else {
                                    break;
                                }
                            }
                            this.currRightNum = 0;
                            this.currLeftNum = 0;
                            this.rightIterator = BNLJOperator.this.getPageIterator(rightTableName);
                            this.bufferPointer = 0;
                            continue;

                        }

                    }

                }





//                // now the stuff thats meant to happen
//                this.leftRecord = retrieveNextLeftRecord();
//                this.rightRecord = retrieveNextRightRecord();
//                this.currRightNum++;

                if (this.rightRecord == null) {
                    continue;
                }
//                if (this.leftRecord == null) {
//                    continue;
//                }

                DataType leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                DataType rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                if (leftJoinValue.equals(rightJoinValue)) {

                    List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                    List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    this.nextRecord = new Record(leftValues);
                    return true;
                }





                } catch (Exception e) {
                return false;
            }
        }
      return false;
    }


    private Record retrieveNextLeftRecord() {
        try{
            while(true) {
              while (this.currLeftNum < BNLJOperator.this.getNumEntriesPerPage(leftTableName)) {
                  if (leftPage.getPageNum() == 0) {
                      this.bufferPointer += 1;
                      this.leftPage = block[this.bufferPointer];
                  }

                  byte[] leftHeader = BNLJOperator.this.getPageHeader(rightTableName,this.leftPage);
                  byte b = leftHeader[this.currLeftNum/8];
                  int bitOffset = 7 - (this.currLeftNum % 8);
                  byte mask = (byte) (1 << bitOffset);
                  byte value = (byte) (b & mask);
                  if (value != 0) {
                      int entrySize = BNLJOperator.this.getEntrySize(leftTableName);
                      int offset = BNLJOperator.this.getHeaderSize((leftTableName)) + (entrySize * currLeftNum);
                      byte[] bytes = this.leftPage.readBytes(offset, entrySize);
                      Record toRtn = BNLJOperator.this.getLeftSource().getOutputSchema().decode(bytes);
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
              while (this.currRightNum < BNLJOperator.this.getNumEntriesPerPage(rightTableName)) {
                  if (rightPage.getPageNum() == 0) { this.rightPage = this.rightIterator.next(); }
                  byte[] rightHeader = BNLJOperator.this.getPageHeader(rightTableName,this.rightPage);
                  byte b = rightHeader[this.currRightNum / 8];
                  int bitOffset = 7 - (this.currRightNum % 8);
                  byte mask = (byte) (1 << bitOffset);
                  byte value = (byte) (b & mask);
                  if (value != 0) {
                      int entrySize = BNLJOperator.this.getEntrySize(rightTableName);
                      int offset = BNLJOperator.this.getHeaderSize((rightTableName)) + (entrySize * currRightNum);
                      byte[] bytes = this.rightPage.readBytes(offset, entrySize);
                      Record toRtn = BNLJOperator.this.getRightSource().getOutputSchema().decode(bytes);
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

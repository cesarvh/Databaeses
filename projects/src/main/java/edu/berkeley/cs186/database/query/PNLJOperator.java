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
    return -1;
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

          this.leftIterator = PNLJOperator.this.getPageIterator(leftTableName);
          this.leftPage = this.leftIterator.next();
          this.rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
          this.rightPage = this.rightIterator.next();



          // TODO: implement me!

      }
    /*

    OUTER join INNER
    O = left
    I = right
    foreach page bo in O do
      for each page bi in I do
        for each record o in bo do
        for each record i in bi do
        if theta() then add <r, s> 

    outer on y axis
    inner on x asis\
      |--------------->
      |   |   |
      |--------------->
    O |   |   |
      |--------------->
      |___|___|_________
            I
    */

    public boolean hasNext() {
      // TODO: implement me!

      while true {

        // for each record in page
        while (this.leftIterator.hasNext()) {
          if (!this.leftPage.hasNext()) { // we iterate through this page ONLY once.
            this.leftPage = this.leftIterator.next();
            this.rightIterator = PNLJOperator.this.getPageIterator(rightTableName);
            this.rightPage = this.rightIterator.next();

          }

          while (this.rightIterator.hasNext()) { // now we iterate through this page
            if (this.rightIterator.hasNext()) {
              this.rightIterator = this.rightIterator.next();
            }


          }

        }



      }
        

      return false;
    }

    public Record retrieveNextLeftRecord() {
      return null;
    }

    public Record retrieveNextRightRecord() {
      return null;
    }


    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {
      // TODO: implement me!
      throw new NoSuchElementException();
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

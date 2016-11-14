package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.stats.TableStats;


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
    return -1;
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

//        while (this.sourceIterator.hasNext()) {
//            Record record = this.sourceIterator.next();
//            DataType groupByColumn = record.getValues().get(GroupByOperator.this.groupByColumnIndex);
//
//            String tableName;
//            if (!this.hashGroupTempTables.containsKey(groupByColumn.toString())) {
//                tableName = "Temp" + GroupByOperator.this.groupByColumn + "GroupBy" + this.hashGroupTempTables.size();
//
//                GroupByOperator.this.transaction.createTempTable(GroupByOperator.this.getSource().getOutputSchema(), tableName);
//                this.hashGroupTempTables.put(groupByColumn.toString(), tableName);
//            } else {
//                tableName = this.hashGroupTempTables.get(groupByColumn.toString());
//            }
//
//            GroupByOperator.this.transaction.addRecord(tableName, record.getValues());
//        }

        while (this.rightIterator.hasNext()) {
            Record r = this.rightIterator.next(); // get the next record, now we will hash it
            DataType column = r.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            int hash = column.hashCode();


            System.out.println(rightPartitions.length);

            for (int i = 0; i < rightPartitions.length; i++) {
                System.out.println(rightPartitions[i]);
            }
            System.out.println("left " +  column + " and hash is " + hash + " and the bucket num is " +  hash % rightPartitions.length);


        }

        while (this.leftIterator.hasNext()) {
            Record r = this.leftIterator.next(); // get the next record, now we will hash it
            DataType column = r.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            int hash = column.hashCode();
            System.out.println("right " + column + " and hash is " + hash + " and the bucket num is " +  hash % leftPartitions.length);
        }


        // hash right


        // hash left



        // on the first half of the algorithm, each record is hashed into its corresponding partition by being
        // added to a temporary table that represents that partition
        // use a datatype's hashcode and modulo operator to hash each into  a particular parition (groupByOperator)




//      for (int i = 0; i < rightPartitions.length; i++) {
//          System.out.println(rightPartitions[i]);
//      }
//        System.out.println("==============Left partitions start now=================");
//        for (int i = 0; i < leftPartitions.length; i++) {
//            System.out.println(leftPartitions[i]);
//        }

    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
      // TODO: implement me!


      // on the second phase,
      // build an in-memory hash table using a HashMap
      // you should use the records form the left input partitions to hash and then probe the hash table using
      // the records from the right input partitions


      return false;
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

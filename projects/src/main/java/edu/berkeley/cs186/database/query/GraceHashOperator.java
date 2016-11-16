package edu.berkeley.cs186.database.query;

import java.util.*;

import com.sun.org.apache.regexp.internal.RE;
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
    private int tracker;
    private int currRight;

      private Record leftRecord;

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

        while (this.rightIterator.hasNext()) {
            Record r = this.rightIterator.next(); // get the next record, now we will hash it
            DataType column = r.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            int hash = column.hashCode();
            int bucket = hash % (numBuffers - 1);

            GraceHashOperator.this.addRecord(rightPartitions[bucket], r.getValues());

        }

        while (this.leftIterator.hasNext()) {
            Record r = this.leftIterator.next(); // get the next record, now we will hash it
            DataType column = r.getValues().get(GraceHashOperator.this.getRightColumnIndex());
            int hash = column.hashCode();
            int bucket = hash % (numBuffers - 1);

            GraceHashOperator.this.addRecord(leftPartitions[bucket], r.getValues());
        }

        this.currentPartition = 0;
        this.inMemoryHashTable = new HashMap<>();

//        this.leftIterator = getLeftSource().iterator();
        this.tracker = 0;
        this.currRight = 0;

    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */


//      // if the hash table is empty, fill it with next left partition
//      if (inMemoryHashTable.isEmpty()) {
//          this.currentPartition++;
//          if (this.currentPartition < this.leftPartitions.length) {
//              try {
//                  this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[this.currentPartition]);
//              } catch (DatabaseException e) {
//                  return false;
//              }
//              while (this.leftIterator.hasNext()) {
//                  Record record = this.leftIterator.next();
//                  DataType joinValue = record.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
//                  ArrayList<Record> records;
//                  if (inMemoryHashTable.containsKey(joinValue)) {
//                      records = inMemoryHashTable.get(joinValue);
//                  } else {
//                      records = new ArrayList<Record>();
//                  }
//                  records.add(record);
//                  inMemoryHashTable.put(joinValue, records);
//              }
//              try {
//                  this.rightIterator = GraceHashOperator.this.getTableIterator(this.rightPartitions[this.currentPartition]);
//              } catch (DatabaseException e) {
//                  return false;
//              }
//              this.currentTuple = 0;
//          } else {
//              return false;
//          }
//      }

    public boolean hasNext() {
        if (this.nextRecord != null) {
            return true;
        } try {
            while (true) {
                // Build Phase:
                if (this.inMemoryHashTable.size() == 0) {

                    this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[this.currentPartition]);
//                    Record t = this.leftPartitions[this.currentPartition];
                    if (!this.leftIterator.hasNext()) {
                        while (!this.leftIterator.hasNext()) {
                            this.currentPartition++;
                            this.leftIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[this.currentPartition]);

                        }
                    }
                    while (this.leftIterator.hasNext()) {
                        Record r = this.leftIterator.next();
                        DataType key = r.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
                        ArrayList<Record> recordList;

                        if (inMemoryHashTable.containsKey(key)) {
                              recordList = inMemoryHashTable.get(key);
                        } else {
                            recordList = new ArrayList<Record>();
                        }
                        recordList.add(r);
                        inMemoryHashTable.put(key, recordList);
                    }

                    this.rightIterator =  GraceHashOperator.this.getTableIterator(this.rightPartitions[this.currentPartition]);
                    this.rightRecord = this.rightIterator.next();
                }



                if (this.inMemoryHashTable.size() > 0 ) {
                    DataType prober = this.rightRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());

                    if (this.tracker < inMemoryHashTable.get(prober).size()) {
                        this.leftRecord = this.inMemoryHashTable.get(prober).get(this.tracker);
                        this.tracker++;

                        // Comparison and joining
                        DataType leftJoinValue = this.leftRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
                        DataType rightJoinValue = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());


                        if (leftJoinValue.equals(rightJoinValue)) {

                            List<DataType> leftValues = new ArrayList<DataType>(this.leftRecord.getValues());
                            List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
                            leftValues.addAll(rightValues);
                            this.nextRecord = new Record(leftValues);

                            return true;
                        }

                    }
//                }
                    else {
                        if (!this.rightIterator.hasNext()) {
//                        this.rightIterator =
                            this.tracker = 0;
                            this.inMemoryHashTable = new HashMap<>();
                            this.currentPartition++;
                            continue;
                        } else {
                            this.rightRecord = this.rightIterator.next();
                            this.tracker = 0;

                        }
                    }
                }





            }
        } catch (Exception e) {
            return false;
        }
//        return false;
    }

//    public void debugPrints() {
//        System.out.println("Currently at left index: " + this.leftRecordIndex);
//    }

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

//      public boolean hasNext() {
//
//          if (this.nextRecord != null) {
//              return true;
//          }
//          try {
//              while (true) {
//                  // Build phase: Initialize the hashmap, and add everything from the ith partition to the hashmap
//                  // And put the records in the arraylist along with it
//                  // I also create a right iterator to begin iterating through the records the first time hasNext() is called
//                  // currRight: A tracker for debugging purposes.
//                  if (this.inMemoryHashTable.size() == 0) {
//
//                      Iterator<Record> partIterator = GraceHashOperator.this.getTableIterator(this.leftPartitions[this.currentPartition]);
//                      Record r = null;
//                      ArrayList<Record> recordList = new ArrayList<>();
//                      while (partIterator.hasNext()) {
//                          r = partIterator.next();
//                          recordList.add(r);
//                      }
//
//                      // put everything in the hash table
//                      DataType column = r.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
//                      inMemoryHashTable.put(column, recordList);
//
//                      // create a new right iterator and advance it to start
//                      this.rightIterator = GraceHashOperator.this.getTableIterator(this.rightPartitions[this.currentPartition]);
//                      this.rightRecord = this.rightIterator.next();
//                      this.currRight++;
//                  }
//
//                  // This is the probing phase. After I build...
//                  if (this.inMemoryHashTable.size() > 0) { // then we dont need to evaluate them
//                      // First I check if the right iterator has more things in it. If it does, then I can do two things:
//                      if (this.rightIterator.hasNext()) {
////                        debugPrints();
//                          // Case 1: We're not done comparing everything in the arraylist to the current Right record,
//                          //         So we get the nextRecord, which is the next left, and increase the leftRecordIndex, which tracks our
//                          //         Position in the arraylist
//                          if (this.leftRecordIndex < this.inMemoryHashTable.get(this.rightRecord.getValues() // Not gone thru everything in the page
//                                  .get(GraceHashOperator.this.getLeftColumnIndex())).size()) {
//                              this.nextRecord = inMemoryHashTable.get(this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex())).get(this.leftRecordIndex);
//                              this.leftRecordIndex++;
//
//                              // Case 2: We're done comparing it to the items in the arraylist, so we move on and go to the next record
//                              //     I increase currRight for debugging, and make LeftRecordIndex = 0 to start at the head of the arraylist again, and compare it
//                          } else {
//                              this.rightRecord = rightIterator.next();
//                              this.currRight++;
//                              this.leftRecordIndex = 0;
//                              this.nextRecord = inMemoryHashTable.get(this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex())).get(this.leftRecordIndex);
////                            this.leftRecordIndex++;
//                          }
//
//                          // Comparison and joining
//                          DataType leftJoinValue = this.nextRecord.getValues().get(GraceHashOperator.this.getLeftColumnIndex());
//                          DataType rightJoinValue = this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex());
//
//
//                          if (leftJoinValue.equals(rightJoinValue)) {
//
//                              List<DataType> leftValues = new ArrayList<DataType>(this.nextRecord.getValues());
//                              List<DataType> rightValues = new ArrayList<DataType>(rightRecord.getValues());
//                              leftValues.addAll(rightValues);
//                              this.nextRecord = new Record(leftValues);
//
//                              return true;
//                          }
//
//                      } else {
//                          // move on to next partition if needed, clearing the hashmap
//                          this.currentPartition++;
//                          this.inMemoryHashTable = new HashMap<>();
//
//                      }
//
//                  }
//
//              }
//
//          } catch (Exception e) {
//              return false;
//          }
//
//
//
//
//          // on the second phase,
//          // build an in-memory hash table using a HashMap
//          // you should use the records form the left input partitions to hash and then probe the hash table using
//          // the records from the right input partitions
//
//
////      return false;
//      }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}

package edu.berkeley.cs186.database.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

public class IndexScanOperator extends QueryOperator {
  private Database.Transaction transaction;
  private String tableName;
  private String columnName;
  private QueryPlan.PredicateOperator predicate;
  private DataType value;

  private int sourceColumnIndex;

  /**
   * An index scan operator.
   *
   * @param transaction the transaction containing this operator
   * @param tableName the table to iterate over
   * @param columnName the name of the column the index is on
   * @throws QueryPlanException
   * @throws DatabaseException
   */
  public IndexScanOperator(Database.Transaction transaction,
                           String tableName,
                           String columnName,
                           QueryPlan.PredicateOperator predicate,
                           DataType value) throws QueryPlanException, DatabaseException {
    super(OperatorType.INDEXSCAN);
    this.tableName = tableName;
    this.transaction = transaction;
    this.columnName = columnName;
    this.predicate = predicate;
    this.value = value;

    this.setOutputSchema(this.computeSchema());

    columnName = this.checkSchemaForColumn(this.getOutputSchema(), columnName);
    this.sourceColumnIndex = this.getOutputSchema().getFieldNames().indexOf(columnName);
  }

  public Iterator<Record> execute() throws DatabaseException {
    if (this.predicate == QueryPlan.PredicateOperator.EQUALS) {
      // if equality search, just use a key lookup

      return this.transaction.lookupKey(this.tableName, this.columnName, this.value);
    } else if (this.predicate == QueryPlan.PredicateOperator.LESS_THAN) {
      // if less than, scan until you get any values >= to the boundary value

      Iterator<Record> recordIterator = this.transaction.sortedScan(this.tableName, this.columnName);
      List<Record> recordList = new ArrayList<Record>();

      while (recordIterator.hasNext()) {
        Record record = recordIterator.next();

        if (record.getValues().get(this.sourceColumnIndex).compareTo(this.value) >= 0) {
          break;
        }

        recordList.add(record);
      }

      return recordList.iterator();
    } else if (this.predicate == QueryPlan.PredicateOperator.LESS_THAN_EQUALS){
      // if less than or equals, scan until you get values > the boundary valeu

      Iterator<Record> recordIterator = this.transaction.sortedScan(this.tableName, this.columnName);
      List<Record> recordList = new ArrayList<Record>();

      while (recordIterator.hasNext()) {
        Record record = recordIterator.next();

        if (record.getValues().get(this.sourceColumnIndex).compareTo(this.value) > 0) {
          break;
        }

        recordList.add(record);
      }

      return recordList.iterator();
    } else {
      // if greater than or equals, sortedScanFrom works just find
      Iterator<Record> recordIterator = this.transaction.sortedScanFrom(this.tableName, this.columnName, this.value);

      // if greater than, get rid of all equal values first
      if (this.predicate == QueryPlan.PredicateOperator.GREATER_THAN) {
        List<Record> recordList = new ArrayList<Record>();

        Record record = null;
        while (recordIterator.hasNext()) {
          record = recordIterator.next();

          if (record.getValues().get(this.sourceColumnIndex).compareTo(this.value) > 0) {
            break;
          }
        }

        if (record != null) {
          recordList.add(record);
        }

        while (recordIterator.hasNext()) {
          recordList.add(recordIterator.next());
        }

        return recordList.iterator();
      }

      return recordIterator;
    }
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException de) {
      throw new QueryPlanException(de);
    }
  }
}

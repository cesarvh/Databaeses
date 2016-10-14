package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.datatypes.BoolDataType;
import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.datatypes.FloatDataType;
import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.datatypes.StringDataType;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.*;

import java.io.Closeable;
import java.util.concurrent.Exchanger;

/**
 * A database table. Allows the user to add, delete, update, and get records.
 * A table has an associated schema, stats, and page allocator. The first page
 * in the page allocator is a header page that serializes the schema, and each
 * subsequent page is a data page containing the table records.
 *
 * Properties:
 * `schema`: the Schema (column names and column types) for this table
 * `freePages`: a set of page numbers that correspond to allocated pages with free space
 * `stats`: the TableStats for this table
 * `allocator`: the PageAllocator for this table
 * `tableName`: name of this table
 * `numEntriesPerPage`: number of records a data page of this table can hold
 * `pageHeaderSize`: physical size (in bytes) of a page header slot bitmap
 * `numRecords`: number of records currently contained in this table
 */
public class Table implements Iterable<Record>, Closeable {
  public static final String FILENAME_PREFIX = "db";
  public static final String FILENAME_EXTENSION = ".table";

  private Schema schema;
  private TreeSet<Integer> freePages;

  private TableStats stats;

  private PageAllocator allocator;
  private String tableName;

  private int numEntriesPerPage;
  private int pageHeaderSize;
  private long numRecords;
//  private ArrayList<RecordID> recordIds;// = new ArrayList<RecordID>();

  public Table(String tableName) {
    this(tableName, FILENAME_PREFIX);
  }

  public Table(String tableName, String filenamePrefix) {


    this.tableName = tableName;
//    this.recordIds = new ArrayList<RecordID>();

    String pathname = Paths.get(filenamePrefix, tableName + FILENAME_EXTENSION).toString();
    this.allocator = new PageAllocator(pathname, false);
    this.readHeaderPage();

    this.stats = new TableStats(this.schema);

    this.freePages = new TreeSet<Integer>();
    this.setEntryCounts();
    Iterator<Page> pIter = this.allocator.iterator();
    pIter.next();

    long freshCountRecords = 0;
    int pageNum = 0; // Page count
    while(pIter.hasNext()) {
      Page p = pIter.next();
      pageNum++;

      // add all records in this page to TableStats
      int entryNum = 0;
      byte[] header = this.readPageHeader(p);
      while (entryNum < this.numEntriesPerPage) {
        byte b = header[entryNum/8];
        int bitOffset = 7 - (entryNum % 8);
        byte mask = (byte) (1 << bitOffset);

        byte value = (byte) (b & mask);
        if (value != 0) {
          RecordID rid = new RecordID(pageNum, entryNum);
          int entrySize = this.schema.getEntrySize();

          int offset = this.pageHeaderSize + (entrySize * entryNum);
          byte[] bytes = p.readBytes(offset, entrySize);

          Record record = this.schema.decode(bytes);
          entryNum++;

          this.stats.addRecord(record);
//          this.recordIds.add(rid);
        }

        entryNum++;
      }

      if (spaceOnPage(p)) {
        this.freePages.add(p.getPageNum());
      }

      freshCountRecords += numValidEntries(p);
    }

    this.numRecords = freshCountRecords;
  }

  public Table(Schema schema, String tableName) {
    this(schema, tableName, FILENAME_PREFIX);
  }

  /**
   * This constructor is used for creating a table in some specified directory.
   *
   * @param schema the schema for this table
   * @param tableName the name of the table
   * @param filenamePrefix the prefix where the table's files will be created
   */
  public Table(Schema schema, String tableName, String filenamePrefix) {
    this.schema = schema;
    this.tableName = tableName;
    this.stats = new TableStats(this.schema);
//    this.recordIds = new ArrayList<RecordID>();
    this.freePages = new TreeSet<Integer>();
    String pathname = Paths.get(filenamePrefix, tableName + FILENAME_EXTENSION).toString();
    this.allocator = new PageAllocator(pathname, true);

    this.setEntryCounts();

    this.writeHeaderPage();
  }

  public void close() {
    allocator.close();
  }

  public Iterator<Record> iterator() {
      return new TableIterator();
  }

  /**
   * Add a new record to this table. The record should be added to the first
   * free slot of the first free page if one exists, otherwise a new page should
   * be allocated and the record should be placed in the first slot of that
   * page. Recall that a free slot in the slot bitmap means the bit is set to 0.
   * Make sure to update this.stats, this.freePages, and this.numRecords as
   * necessary.
   *
   * @param values the values of the record being added
   * @return the RecordID of the added record
   * @throws DatabaseException if the values passed in to this method do not
   *         correspond to the schema of this table
   */
  public RecordID addRecord(List<DataType> values) throws DatabaseException {
    Record record = null;

    try {
        record = this.schema.verify(values);
    } catch (SchemaException e) {
        throw new DatabaseException("Values passed do not correspond to the schema of this table");
    }

    int pageNum;
    if (this.freePages.size() == 0) {
        pageNum = this.allocator.allocPage();
    } else {
        pageNum = this.freePages.first();
    }
    byte[] encoded = this.schema.encode(record);
    Page page = this.allocator.fetchPage(pageNum);
    byte[] header = this.readPageHeader(page);

    int slotNumber = 0;
    int slotIsValid = 1;
    int position = 0;

    while (slotNumber < this.numEntriesPerPage && slotIsValid != 0) {
      byte b = header[slotNumber / 8];
      int bitOffset = 7 - (slotNumber % 8);
      byte mask = (byte) (1 << bitOffset);

      slotIsValid = (b & mask);
      if (slotIsValid == 0) {
        position = this.pageHeaderSize + (this.schema.getEntrySize() * slotNumber);
        break;

      } else {
        slotNumber += 1;
      }
    }

    this.writeBitToHeader(page, slotNumber, (byte) 1);
    page.writeBytes(position, this.schema.getEntrySize(), encoded);


    if (!spaceOnPage(page)) {
      freePages.remove(pageNum);
    } else {
      freePages.add(pageNum);
    }

    RecordID rid = new RecordID(pageNum, slotNumber);

    this.numRecords += 1;
    this.stats.addRecord(record);

//    this.recordIds.add(rid);

    return rid;


  }

  /**
   * Deletes the record specified by rid from the table. Make sure to update
   * this.stats, this.freePages, and this.numRecords as necessary.
   *
   * @param rid the RecordID of the record to delete
   * @return the Record referenced by rid that was removed
   * @throws DatabaseException if rid does not correspond to a valid record
   */
  public Record deleteRecord(RecordID rid) throws DatabaseException {
      try {
          int pageNum = rid.getPageNum();
          int slotNum = rid.getSlotNumber();
          Page page = this.allocator.fetchPage(pageNum);
          Record record = this.getRecord(rid);
          writeBitToHeader(page, slotNum, (byte) 0);

          this.numRecords -= 1;
          this.stats.removeRecord(record);

//          this.recordIds.remove(rid);
          return record;
      } catch (Exception e) {
          throw new DatabaseException("Invalid record id");
      }

  }

  /**
   * Retrieves a record from the table.
   *
   * @param rid the RecordID of the record to retrieve
   * @return the Record referenced by rid
   * @throws DatabaseException if rid does not correspond to a valid record
   */
  public Record getRecord(RecordID rid) throws DatabaseException {
    boolean valid;
    try {
        valid = this.checkRecordIDValidity(rid);
    } catch (DatabaseException e) {
        // System.out.println();
        throw new DatabaseException("stop");
    }
    if (!valid) {
        throw new DatabaseException("stop");
    }



    int pageNum = rid.getPageNum();
    int slotNumber = rid.getSlotNumber();
    int offset = this.pageHeaderSize + (slotNumber * this.schema.getEntrySize());
    Page page = this.allocator.fetchPage(pageNum);

    byte[] pageBytes = page.readBytes(offset, this.schema.getEntrySize());
    return this.schema.decode(pageBytes);

  }

  /**
   * Update an existing record with new values. Make sure to update this.stats
   * as necessary.
   *
   * @param values the new values of the record
   * @param rid the RecordID of the record to update
   * @return the old version of the record
   * @throws DatabaseException if rid does not correspond to a valid record or
   *         if the values do not correspond to the schema of this table
   */
  public Record updateRecord(List<DataType> values, RecordID rid) throws DatabaseException {
//    TODO: Implement Me!!
        Record record = null;
      if (!checkRecordIDValidity(rid)) {
          throw new DatabaseException("Something went wrong");
      }
      try {
          record = this.schema.verify(values);
      } catch (Exception e){
          throw new DatabaseException("Invalid record");
      }


      int pageNum = rid.getPageNum();
      int slotnNum = rid.getSlotNumber();
      Page page = this.allocator.fetchPage(pageNum);
      byte[] encoded = this.schema.encode(record);
      Record oldRecord = this.getRecord(rid);

      int position = this.schema.getEntrySize() * slotnNum + this.pageHeaderSize;

      page.readBytes();
      page.writeBytes(position, this.schema.getEntrySize(), encoded);

      return oldRecord;
  }

  public int getNumEntriesPerPage() {
    return this.numEntriesPerPage;
  }

  public Schema getSchema() {
    return this.schema;
  }

  /**
   * Check whether a RecordID is valid or not. That is, check to see if the slot
   * in the page specified by the RecordID contains a valid record (i.e. whether
   * the bit in the slot bitmap is set to 1).
   *
   * @param rid the record id to check
   * @return true if rid corresponds to a valid record, otherwise false
   * @throws DatabaseException if rid does not reference an existing data page slot
   */
    private boolean checkRecordIDValidity(RecordID rid) throws DatabaseException {
        // if header
        try {
          int slotNumber = rid.getSlotNumber();
          int pageNum = rid.getPageNum();

          if (slotNumber <= this.getNumEntriesPerPage()) {
            Page page = this.allocator.fetchPage(pageNum);
            byte[] header = this.readPageHeader(page);

            int byteOffset = slotNumber / 8;
            int bitOffset = 7 - (slotNumber % 8);
            byte mask = (byte) (1 << bitOffset);
            byte res = (byte) (header[byteOffset] & mask);

            if (res ==  (byte) 0) {
              return false;
            } else {
              return true;
            }

          } else {
            throw new DatabaseException("No");
          }

        } catch (PageException e) {
          throw new DatabaseException("Not a valid record");
        }


    }

  /**
   * Based on the Schema known to this table, calculate the number of record
   * entries a data page can hold and the size (in bytes) of the page header.
   * The page header only contains the slot bitmap and takes up no other space.
   * For ease of calculations and to prevent header byte splitting, ensure that
   * `numEntriesPerPage` is a multiple of 8 (this may waste some space).
   *
   * Should set this.pageHeaderSize and this.numEntriesPerPage.
   */
  private void setEntryCounts() {
    this.numEntriesPerPage = Page.pageSize / (this.schema.getEntrySize() + 1/8);

    if (this.numEntriesPerPage % 8 != 0){
        this.numEntriesPerPage = this.numEntriesPerPage - (this.numEntriesPerPage % 8);
    }

    while (this.numEntriesPerPage*this.schema.getEntrySize() + (this.numEntriesPerPage/8) > Page.pageSize){
        this.numEntriesPerPage = this.numEntriesPerPage - 8;
    }
    this.pageHeaderSize = this.numEntriesPerPage / 8;


    }

  /**
   * Checks if there is any free space on the given page.
   *
   * @param p the page to check
   * @return true if there exists free space, otherwise false
   */
  private boolean spaceOnPage(Page p) {
    byte[] header = this.readPageHeader(p);

    for (byte b : header) {
      if (b != (byte) 0xFF) {
        return true;
      }
    }

    return false;
  }

  /**
   * Checks how many valid record entries are in the given page.
   *
   * @param p the page to check
   * @return number of record entries in p
   */
  private int numValidEntries(Page p) {
    byte[] header = this.readPageHeader(p);
    int count = 0;

    for (byte b : header) {
      for (int mask = 0x01; mask != 0x100; mask <<= 1) {
        if ((b & (byte) mask) != 0) {
          count++;
        }
      }
    }

    return count;
  }

  /**
   * Utility method to write the header page of the table. The only information written into
   * the header page is the table's schema.
   */
  private void writeHeaderPage() {
    int numBytesWritten = 0;
    Page headerPage = this.allocator.fetchPage(this.allocator.allocPage());

    assert(0 == headerPage.getPageNum());

    List<String> fieldNames = this.schema.getFieldNames();
    headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(fieldNames.size()).array());
    numBytesWritten += 4;

    for (String fieldName : fieldNames) {
      headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(fieldName.length()).array());
      numBytesWritten += 4;
    }

    for (String fieldName : fieldNames) {
      headerPage.writeBytes(numBytesWritten, fieldName.length(), fieldName.getBytes(Charset.forName("UTF-8")));
      numBytesWritten += fieldName.length();
    }

    for (DataType field : this.schema.getFieldTypes()) {
      headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(field.type().ordinal()).array());
      numBytesWritten += 4;

      if (field.type().equals(DataType.Types.STRING)) {
        headerPage.writeBytes(numBytesWritten, 4, ByteBuffer.allocate(4).putInt(field.getSize()).array());
        numBytesWritten += 4;
      }
    }
  }

  public long numRecords() {
    return this.numRecords;
  }

  /**
   * Utility method to read the header page of the table.
   */
  private byte[] readHeaderPage() {
    int numBytesRead = 0;
    Page headerPage = this.allocator.fetchPage(0);

    int numFields = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
    numBytesRead += 4;

    List<Integer> fieldNameLengths = new ArrayList<Integer>();
    for (int i = 0; i < numFields; i++) {
      fieldNameLengths.add(ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt());
      numBytesRead += 4;
    }

    List<String> fieldNames = new ArrayList<String>();
    for (int fieldNameLength : fieldNameLengths) {
      byte[] bytes = headerPage.readBytes(numBytesRead, fieldNameLength);

      fieldNames.add(new String(bytes, Charset.forName("UTF-8")));
      numBytesRead += fieldNameLength;
    }

    List<DataType> fieldTypes = new ArrayList<DataType>();
    for (int i = 0; i < numFields; i++) {
      int ordinal = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
      DataType.Types type = DataType.Types.values()[ordinal];
      numBytesRead += 4;

      switch(type) {
        case INT:
          fieldTypes.add(new IntDataType());
          break;
        case STRING:
          int len = ByteBuffer.wrap(headerPage.readBytes(numBytesRead, 4)).getInt();
          numBytesRead += 4;

          fieldTypes.add(new StringDataType(len));
          break;
        case BOOL:
          fieldTypes.add(new BoolDataType());
          break;
        case FLOAT:
          fieldTypes.add(new FloatDataType());
          break;
      }
    }

    this.schema = new Schema(fieldNames, fieldTypes);

    return new byte[0];
  }

  /**
   * Utility method to write a particular bit into the header of a particular page.
   *
   * @param page the page to modify
   * @param slotNum the header slot to modify
   * @param value the value of the bit to write (should either be 0 or 1)
   */
      // this.writeBitToHeader(page, freeSlotNumber, 1);
  private void writeBitToHeader(Page page, int slotNum, byte value) {
    byte[] header = this.readPageHeader(page);
    int byteOffset = slotNum / 8;
    int bitOffset = 7 - (slotNum % 8);

    if (value == 0) {
      byte mask = (byte) ~((1 << bitOffset));

      header[byteOffset] = (byte) (header[byteOffset] & mask);
      page.writeBytes(0, this.pageHeaderSize, header);
    } else {
      byte mask = (byte) (1 << bitOffset);

      header[byteOffset] = (byte) (header[byteOffset] | mask);
    }

    page.writeBytes(0, this.pageHeaderSize, header);
  }

  /**
   * Read the slot header of a page.
   *
   * @param page the page to read from
   * @return a byte[] with the slot header
   */
  private byte[] readPageHeader(Page page) {
    return page.readBytes(0, this.pageHeaderSize);
  }

  /**
   * An implementation of Iterator that provides an iterator interface over all
   * of the records in this table.
   */
  private class TableIterator implements Iterator<Record> {
      int cursor;
      int recordNum;
      int pageNum;
      Page currentPage;
      Iterator<Page> pIter;

    public TableIterator() {
      this.cursor = 0;
      this.recordNum = 0;
      this.pageNum = 1;
      this.pIter = Table.this.allocator.iterator();
      this.currentPage = null;
      if (this.pIter.hasNext()) {this.pIter.next();}
    }

    /**
     * Checks if there are more record(s) to yield
     *
     * @return true if this iterator has another record to yield, otherwise false
     */
    public boolean hasNext() {
        return this.recordNum < Table.this.numRecords;
    }

    /**
     * Yields the next record of this iterator.
     *
     * @return the next Record
     * @throws NoSuchElementException if there are no more Records to yield
     */
    public Record next() {

    if (this.hasNext()) {
      Record record = null;
//      if (this.cursor > Table.this.numEntriesPerPage - 1) {
//        if (this.pIter.hasNext()) {
////          this.currentPage = this.pIter.next();
//          this.pageNum++;
//          this.cursor = 0;
//        } else {
//          return null;
//        }
//      }
      while (record == null) {
        try {
          if (this.cursor > Table.this.numEntriesPerPage - 1) {
            if (this.pIter.hasNext()) {
//              this.currentPage = this.pIter.next();
              this.pageNum++;
              this.cursor = 0;
            }
          }
          RecordID rid = new RecordID(this.pageNum, this.cursor);
          record = Table.this.getRecord(rid);
          this.cursor++;
        } catch (Exception e) {
            cursor++;
        }
      }
      this.recordNum++;
      return record;
    }
    return null;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}
}

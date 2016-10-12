package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;
import jdk.nashorn.internal.ir.BaseNode;
import org.relaxng.datatype.Datatype;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree leaf node. A leaf node header contains the page number of the
 * parent node (or -1 if no parent exists), the page number of the previous leaf
 * node (or -1 if no previous leaf exists), and the page number of the next leaf
 * node (or -1 if no next leaf exists). A leaf node contains LeafEntry's.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class LeafNode extends BPlusNode {

  public LeafNode(BPlusTree tree) {
    super(tree, true);
    getPage().writeByte(0, (byte) 1);
    setPrevLeaf(-1);
    setParent(-1);
    setNextLeaf(-1);
  }
  
  public LeafNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, true);
    if (getPage().readByte(0) != (byte) 1) {
      throw new BPlusTreeException("Page is not Leaf Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
      int pointerNum;
      BPlusNode pointerNode = null;
      LeafNode currentNode = this;

      List<BEntry> entries = this.getAllValidEntries();
      if (!scanForKey(key).hasNext()) {
          return this;
      }


      for (int i = 0; i < entries.size(); i++) {
          LeafEntry currentEntry = (LeafEntry) entries.get(i);
          DataType currentEntryKey = currentEntry.getKey();

          if (key.compareTo(currentEntryKey) == 0) {
//              pointerNum = currentEntry.getPageNum();
              pointerNode = getBPlusNode(this.getTree(), this.getPageNum());
              if (!findFirst) {
                  return (LeafNode) pointerNode;
              } else {
                  // then we walk backwards
                   while (true) {
                       pointerNum = currentNode.getPrevLeaf();
                       if (pointerNum == -1) {
                           return (LeafNode) pointerNode;
                       }
                       pointerNode = BPlusNode.getBPlusNode(this.getTree(), pointerNum);
                   }



              }
          }

//          else if (key.compareTo(currentEntryKey) == 1) {
//              pointerNum = currentNode.getNextLeaf();
//              pointerNode = BPlusNode.getBPlusNode(this.getTree(), pointerNum);
//
//          }
//
//          else if (key.compareTo(currentEntryKey) == -1) {
//              pointerNum = currentNode.getPrevLeaf();
//              pointerNode = BPlusNode.getBPlusNode(this.getTree(), pointerNum);
//
//          }

      }
      return (LeafNode) pointerNode;
//          currentNode = (LeafNode) pointerNode;

//          return locateLeafHelper(currentNode, key, findFirst);



  }



    /**
   * Splits this node and copies up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full leaf node of 2d entries will be split
   * into a left node with d entries and a right node with d entries, with the
   * leftmost key of the right node copied up.
   */
  @Override
  public void splitNode() {
      LeafNode rightNode = new LeafNode(this.getTree(), this.getPageNum());
//      InnerNode parentNode = (InnerNode) getBPlusNode(this.getTree(), this.getParent());
//      InnerNode newParent = new InnerNode(this.getTree(), parentNode.getPageNum());

      List<BEntry> entries = this.getAllValidEntries();
      List<BEntry> rightEntries = new ArrayList<BEntry>();
      List<BEntry> leftEntries = new ArrayList<BEntry>();


      BEntry middleEntry = entries.get(entries.size()/2);
//      DataType middleKey = middleEntry.getKey();

      for (int i = 0; i < entries.size()/2 ; i++) {
          leftEntries.add(entries.get(i));
      }
      for (int j = entries.size()/2; j < entries.size(); j++) {
          rightEntries.add(entries.get(j));
      }
      DataType middleKey = rightEntries.get(0).getKey();
      rightNode.overwriteBNodeEntries(rightEntries);
//      int middlePage = rightEntries.get(0).getPageNum();

//      InnerEntry tempEntry = new InnerEntry(middleKey, middleEntry.);

      this.overwriteBNodeEntries(leftEntries);

      rightNode.setPrevLeaf(this.getPageNum());
      rightNode.setNextLeaf(BPlusNode.getBPlusNode(this.getTree(), this.getNextLeaf()).getPageNum());
      this.setNextLeaf(rightNode.getPageNum());

//      tempEntry.
//      rightNode.setParent(tempEntry.getPageNum());
//      tempEntry. setFirstChild(middleEntry.getPageNum());

//      inse
//      insertBEntry(tempEntry);
//      insert
//
//      System.out.println(rightEntries.size());
//      System.out.println(leftEntries.size());
//      rightNode.setParent();
//      leftNode.setParent();

//      BPlusNode parent = BPlusNode.getBPlusNode(this.getTree(), this.getParent());
//      List<BEntry> pEntries = parent.getAllValidEntries();
//      pEntries.add(middleEntry);

//      parent.overwriteBNodeEntries();

//      All nodes have a setParent function so for the newly created leaf node on the right side you can just call setParent.

//      To handle the relationship parent --> child, each entry has a pagenumber field.
//      That page number is the page number of that entries child. So of the inner node entry you want to set its page number reference to the right side.
//      System.out.println(entries.toString());
//      System.out.println(middleKey + "<==== is the middle key. This is a Leaf Node");


  }

  public int getPrevLeaf() {
    return getPage().readInt(5);
  }

  public int getNextLeaf() {
    return getPage().readInt(9);
  }
  
  public void setPrevLeaf(int val) {
    getPage().writeInt(5, val);
  }

  public void setNextLeaf(int val) {
    getPage().writeInt(9, val);
  }

  /**
   * Creates an iterator of RecordID's for all entries in this node.
   *
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scan() {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      rids.add(le.getRecordID());
    }

    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's whose keys are greater than or equal to
   * the given start value key.
   *
   * @param startValue the start value key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanFrom(DataType startValue) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (startValue.compareTo(le.getKey()) < 1) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's that correspond to the given key.
   *
   * @param key the search key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanForKey(DataType key) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (key.compareTo(le.getKey()) == 0) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }
}

package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;
//import jdk.nashorn.internal.ir.BaseNode;
//import org.relaxng.datatype.Datatype;

import java.util.*;

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

      if (findFirst) {
          // look back
          if (this.getPrevLeaf() != -1 ){
              LeafNode previousLeaf = (LeafNode) BPlusNode.getBPlusNode(this.getTree(), this.getPrevLeaf());
              List<BEntry> prevEntries = previousLeaf.getAllValidEntries();
              if (key.compareTo(prevEntries.get(prevEntries.size() - 1).getKey()) <= 0) {
                  return  BPlusNode.getBPlusNode(this.getTree(), this.getPrevLeaf()).locateLeaf(key, findFirst);
              }
              return this;
          }
          return this;

      } else{
          if (this.getNextLeaf() != -1) {
              LeafNode nextLeaf = (LeafNode) BPlusNode.getBPlusNode(this.getTree(), this.getNextLeaf());
              List<BEntry> nextEntries = nextLeaf.getAllValidEntries();
              if (key.compareTo(nextEntries.get(0).getKey()) >= 0) {
                  return  BPlusNode.getBPlusNode(this.getTree(), this.getNextLeaf()).locateLeaf(key,findFirst);
              }
              return this;
          }
          return this;

      }


  }



    /**
   * Splits this node and copies up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full leaf node of 2d entries will be split
   * into a left node with d entries and a right node with d entries, with the
   * leftmost key of the right node copied up.
   */
  @Override // CORRECT, I THINK!!!!!!!!!!!!!!
  public void splitNode() {


      InnerNode newRoot;
      LeafNode rightLeaf = new LeafNode(this.getTree());

      List<BEntry> entries = this.getAllValidEntries();
      List<BEntry> rightEntries = new ArrayList<BEntry>();
      List<BEntry> leftEntries = new ArrayList<BEntry>();


      int i = 0;
      for (; i < entries.size()/2; i++) {
          leftEntries.add(entries.get(i));
      }
      for (; i < entries.size(); i++) {
          rightEntries.add(entries.get(i));
      }

//      Collections.sort(rightEntries);
//      Collections.sort(leftEntries);

      this.overwriteBNodeEntries(leftEntries);
      rightLeaf.overwriteBNodeEntries(rightEntries);

      if (this.isRoot()) {
          newRoot = new InnerNode(this.getTree());
          newRoot.setFirstChild(this.getPageNum());
          this.getTree().updateRoot(newRoot.getPageNum());

      } else {
           newRoot = (InnerNode) BPlusNode.getBPlusNode(this.getTree(), this.getParent());
      }

      this.setParent(newRoot.getPageNum());


      rightLeaf.setNextLeaf(this.getNextLeaf());
      rightLeaf.setPrevLeaf(this.getPageNum());
      rightLeaf.setParent(newRoot.getPageNum());


      if (this.getNextLeaf() > 0) {
          LeafNode tempnode = (LeafNode)BPlusNode.getBPlusNode(this.getTree(),this.getNextLeaf());
          tempnode.setPrevLeaf(rightLeaf.getPageNum());
      }

      this.setNextLeaf(rightLeaf.getPageNum());

      InnerEntry newIEntry = new InnerEntry(entries.get(entries.size() / 2).getKey() , rightLeaf.getPageNum());
      newRoot.insertBEntry(newIEntry);



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

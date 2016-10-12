package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import org.relaxng.datatype.Datatype;

import java.util.List;

/**
 * A B+ tree inner node. An inner node header contains the page number of the
 * parent node (or -1 if no parent exists), and the page number of the first
 * child node (or -1 if no child exists). An inner node contains InnerEntry's.
 * Note that an inner node can have duplicate keys if a key spans multiple leaf
 * pages.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {

  public InnerNode(BPlusTree tree) {
    super(tree, false);
    getPage().writeByte(0, (byte) 0);
    setFirstChild(-1);
    setParent(-1);
  }
  
  public InnerNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, false);
    if (getPage().readByte(0) != (byte) 0) {
      throw new BPlusTreeException("Page is not Inner Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  public int getFirstChild() {
    return getPage().readInt(5);
  }
  
  public void setFirstChild(int val) {
    getPage().writeInt(5, val);
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
//    BPlusTree tree = this.getTree();
//    BPlusNode root = BPlusNode.getBPlusNode(this.getTree(), tree.rootPageNum)

    return (LeafNode) locateLeafHelper(this, key, findFirst);
  }

  public BPlusNode locateLeafHelper(BPlusNode currentNode, DataType key, boolean findFirst) {
      List<BEntry> entries = currentNode.getAllValidEntries(); // Get all the valid entries of the thingy

      if (currentNode.isLeaf()) {
        return (LeafNode) currentNode.locateLeaf(key, findFirst);
      }

      for (int i = 0; i < entries.size(); i++) {
          BEntry currentEntry = entries.get(i);

          if (key.compareTo(currentEntry.getKey()) == 0) {
              currentNode = BPlusNode.getBPlusNode(this.getTree(), currentEntry.getPageNum());
          }
//          else if (key.compareTo(currentEntry.getKey()) == -1) {
//              currentNode = BPlusNode.getBPlusNode(this.getTree(), this.getFirstChild());
//          }
      }
    return locateLeafHelper(currentNode, key, findFirst);
  }

  /**
   * Splits this node and pushes up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full inner node of 2d entries will be split
   * into a left node with d entries and a right node with d-1 entries, with the
   * middle key pushed up.
   */
  @Override
  public void splitNode() {
      List<BEntry> entries = this.getAllValidEntries();
      BEntry middleEntry = entries.get(entries.size()/2);
      DataType middleKey = middleEntry.getKey();

      System.out.println(middleKey + "<==== is the middle key. This is an inner node");


  }
}

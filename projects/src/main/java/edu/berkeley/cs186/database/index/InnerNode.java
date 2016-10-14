package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
//import org.relaxng.datatype.Datatype;

import java.util.ArrayList;
import java.util.Collection;
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

      List<BEntry> entries = this.getAllValidEntries();
      BEntry ret;
      int first =key.compareTo(entries.get(0).getKey());
      int last = key.compareTo(entries.get(entries.size() - 1).getKey());


      if (first == -1) {
          return BPlusNode.getBPlusNode(this.getTree(), this.getFirstChild()).locateLeaf(key, findFirst);
      }

      if (findFirst) {
          if ((first == last) || (first == 0 && last == -1)) {
              return BPlusNode.getBPlusNode(this.getTree(), this.getFirstChild()).locateLeaf(key, findFirst);
          }

          else if ((first == 1 && last == 0) || (first == 1 && last == -1)) {
              ret = findFirstOrLast(entries, key, true);
              return BPlusNode.getBPlusNode(this.getTree(), ret.getPageNum()).locateLeaf(key, findFirst);
          }


      } else {
          if ((first == last) || (first == 1 && last == 0)) { // order doesnt matter
              return BPlusNode.getBPlusNode(this.getTree(), entries.get(entries.size() - 1).getPageNum()).locateLeaf(key, findFirst);
          }
          else if ((first == 1 && last == -1) || (first  == 0 && last == -1)) {
              ret = findFirstOrLast(entries, key, false);
              return BPlusNode.getBPlusNode(this.getTree(), ret.getPageNum()).locateLeaf(key, findFirst);
          }
      }
      return null;


  }



    public BEntry findFirstOrLast(List<BEntry> entries, DataType key, boolean findFirst) {
        BEntry currentEntry;
        DataType currentKey;

        List<DataType> keys = new ArrayList<DataType>();

        if (findFirst) {
            for (int i = 0; i < entries.size(); i++) {
                currentEntry = entries.get(i);
                currentKey = currentEntry.getKey();
                if (key.compareTo(currentKey) == 0) {
                    return currentEntry;
                }
            }
        } else {
            for (int i = entries.size() - 1; i > -1; i--) {
                currentEntry = entries.get(i);
                currentKey = currentEntry.getKey();
                if (key.compareTo(currentKey) == 0) {
                    return currentEntry;
                }
            }
        }

        // WHAT IF we don't find the key inside?
        for (int i = 0; i < entries.size(); i++) {
            keys.add(entries.get(i).getKey());
        }

        if (!keys.contains(key)) {
            for (int i = 0, j = 1; j < entries.size(); i++, j++) {
                BEntry pointerEntry = entries.get(i);
                BEntry lookaheadEntry = entries.get(j);
                DataType pointerKey = pointerEntry.getKey();
                DataType lookaheadKey = lookaheadEntry.getKey();

                if (key.compareTo(pointerKey) == 1 && key.compareTo(lookaheadKey) == -1) {
                    return pointerEntry;
                }

            }
        }
        return null;
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
      BPlusTree tree = this.getTree();

      List<BEntry> entries = this.getAllValidEntries();
      List<BEntry> rightEntries = new ArrayList<BEntry>();
      List<BEntry> leftEntries = new ArrayList<BEntry>();

      InnerNode rightChild = new InnerNode(tree); // and this too
      InnerNode newRoot;// = new InnerNode(tree);
      BEntry middleEntry = entries.get(entries.size()/ 2);

      int i = 0;
      for (; i < entries.size() / 2; i++) {
          leftEntries.add(entries.get(i));
      }
      i ++;

      for (; i < entries.size(); i++) {
          BPlusNode currentNode = BPlusNode.getBPlusNode(this.getTree(), entries.get(i).getPageNum());
          currentNode.setParent(rightChild.getPageNum());
          rightEntries.add(entries.get(i));

      }

      if (this.isRoot()) {
          newRoot = new InnerNode(this.getTree());
          newRoot.setFirstChild(this.getPageNum());
          this.getTree().updateRoot(newRoot.getPageNum());
      } else {
          newRoot = (InnerNode)BPlusNode.getBPlusNode(this.getTree(),this.getParent());
      }


      rightChild.overwriteBNodeEntries(rightEntries);
      rightChild.setParent(newRoot.getPageNum());
      rightChild.setFirstChild(middleEntry.getPageNum());

      this.overwriteBNodeEntries(leftEntries);
      this.setParent(newRoot.getPageNum());

      InnerEntry newInnerEntry = new InnerEntry(middleEntry.getKey(), rightChild.getPageNum());

//      newRoot.insertBEntry(newInnerEntry);

      List<BEntry> temp = rightChild.getAllValidEntries();
      for (int j = 0; i < temp.size(); j++) {
          BPlusNode currentNode = BPlusNode.getBPlusNode(this.getTree(), temp.get(j).getPageNum());
          currentNode.setParent(rightChild.getPageNum());

      }
      newRoot.insertBEntry(newInnerEntry);


      // ********** UU    UU CCCCCCCCCCCC KK       KK     TTTTTTTTTTT HH     HH  IIIIIIIII      SSSSS
      // ********** UU    UU CCCCCCCCCCC  KK     KK           TT      HH     HH     II         SS
      // **         UU    UU CC           KK    KK            TT      HH     HH     II       SS
      // **         UU    UU CC           KK  KK              TT      HH     HH     II       SS
      // ********** UU    UU CC           KKKKK               TT      HHHHHHHHH     II        SS
      // **         UU    UU CC           KK   KK             TT      HH     HH     II           SS
      // **         UU    UU CC           KK    KK            TT      HH     HH     II            SS
      // **         UU    UU CCCCCCCCCCCC KK     KK           TT      HH     HH     II           SS
      // **          UUUUUU  CCCCCCCCCCCC KK      KK          TT      HH     HH  IIIIIIIII  SSSSS
  }
}

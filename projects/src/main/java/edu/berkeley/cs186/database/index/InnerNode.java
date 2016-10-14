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
      boolean skip = false;
      boolean breakout = false;
      BPlusNode currentNode = null;

      List<BEntry> entries = this.getAllValidEntries();
      int firstCompare = key.compareTo(entries.get(0).getKey());
      int lastCompare = key.compareTo(entries.get(entries.size() -1).getKey());
//       if (entries.size() == 2) {
//           int o = 10;
//       }
      if (key.compareTo(entries.get(0).getKey()) == 1 && key.compareTo(entries.get(entries.size() - 1).getKey()) == 1) {
          currentNode = BPlusNode.getBPlusNode(this.getTree(), entries.get(entries.size()-1).getPageNum());
      }
      else if (key.compareTo(entries.get(0).getKey()) == -1 && key.compareTo(entries.get(entries.size() - 1).getKey()) == -1) {
          currentNode = BPlusNode.getBPlusNode(this.getTree(), this.getFirstChild());
      }
      else if (key.compareTo(entries.get(0).getKey()) == 0 && key.compareTo(entries.get(entries.size() - 1).getKey()) == 0) {
          if (!findFirst){
              currentNode = BPlusNode.getBPlusNode(this.getTree(),entries.get(entries.size() -1).getPageNum());
          }
          currentNode = BPlusNode.getBPlusNode(this.getTree(), this.getFirstChild());
      }
//      else if (key.compareTo(entries.get(0).getKey()) == 1 && key.compareTo(entries.get(entries.size() - 1).getKey()) == 0) {
//          if (!findFirst) {
//              currentNode = BPlusNode.getBPlusNode(this.getTree(),entries.get(entries.size() -1).getPageNum());
//          }
//          currentNode = BPlusNode.getBPlusNode(this.getTree(), this.getFirstChild());
          //skip = true;
//      }
//      else if (firstCompare == 0  && lastCompare == -1) {
//          if (findFirst) {
//              currentNode = BPlusNode.getBPlusNode(this.getTree(), entries.get(0).getPageNum());
//          } else {
//              for (int last = entries.size() -1; last > - 1; last--) {
//                  BEntry currentEntry = entries.get(last);
//                  DataType currentKey = currentEntry.getKey();
//                  if (key.compareTo(currentKey) == 0) {
//                      currentNode = BPlusNode.getBPlusNode(this.getTree(), currentEntry.getPageNum());
//                      break;
//                  }
//              }
//
//          }
//      } else if (firstCompare == 1 && lastCompare == -1 ) {
//            if (findFirst) {
//                for (int i = 0; i < entries.size(); i++) {
//                    BEntry currentEntry = entries.get(i);
//                    DataType currentKey = currentEntry.getKey();
//
//                    if (key.compareTo(currentKey) == 0) {
//                        currentNode = BPlusNode.get
//                    }
//                }
//            }
//            if (!findFirst) {
//
//            }
//      }

//      } else if () {
//
//      }

//

      else if ((firstCompare == 0 || firstCompare == -1) && (firstCompare == 0 || key.compareTo(entries.get(entries.size() - 1).getKey()) == 1)) {
          // this means the wanted page is somewhere inbetween
          if (findFirst) {
              for (int f = 0; f < entries.size(); f++) {
                  BEntry currentEntry = entries.get(f);
                  DataType currentKey = currentEntry.getKey();
                  if (key.compareTo(currentKey) == 0) {
                      currentNode = BPlusNode.getBPlusNode(this.getTree(), currentEntry.getPageNum());
                        break;
                  }
              }
              // find the first one

          } else if (!findFirst) {
              // find the last one
              for (int l = entries.size() -1; l > -1; l--) {
                  BEntry prevEntry = entries.get(l);
                  DataType prevKey = prevEntry.getKey();
                  if (key.compareTo(prevKey) == 0) {
                      currentNode = BPlusNode.getBPlusNode(this.getTree(), prevEntry.getPageNum());
                      break;
                  }
//     if (entries.get(l))
              }

          }
      }

      if (currentNode == null) {
          // something went wrong during asssignment, so we must find the spot where it did!
//          for (int i = 0)
          int i, j;
          for (i = 0, j = 1;  j < entries.size(); j++, i++) {
              BEntry pointer = entries.get(i);
              BEntry lookAhead = entries.get(j);
              DataType pointerKey = pointer.getKey();
              DataType lookAheadKey = lookAhead.getKey();

              if (key.compareTo(pointerKey) == 1 && key.compareTo(lookAheadKey) == -1) {
                  currentNode = BPlusNode.getBPlusNode(this.getTree(), pointer.getPageNum());
                  break;
              }
              else if (key.compareTo(pointerKey) == 1 && key.compareTo(lookAheadKey) == 0) {
                  currentNode = BPlusNode.getBPlusNode(this.getTree(), lookAhead.getPageNum());
                  break;
              }

          }


      }


      if (currentNode.isLeaf()) {
          return (LeafNode) currentNode;
      }

      return currentNode.locateLeaf(key, findFirst);


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

      newRoot.insertBEntry(newInnerEntry);


      // THIS MIGHT ALL BE FUCKING WRONG

      // ********** UU    UU CCCCCCCCCCCC KK       KK     TTTTTTTTTTT HH     HH  IIIIIIIII      SSSSS
      // ********** UU    UU CCCCCCCCCCCC KK     KK           TT      HH     HH     II         SS
      // **         UU    UU CC           KK    KK            TT      HH     HH     II       SS
      // **         UU    UU CC           KK  KK              TT      HH     HH     II       SS
      // ********** UU    UU CC           KKKKK               TT      HHHHHHHHH     II        SS
      // ********** UU    UU CC           KK  KK              TT      HH     HH     II          SS
      // **         UU    UU CC           KK   KK             TT      HH     HH     II           SS
      // **         UU    UU CC           KK    KK            TT      HH     HH     II            SS
      // **         UU    UU CCCCCCCCCCCC KK     KK           TT      HH     HH     II           SS
      // **          UUUUUU  CCCCCCCCCCCC KK      KK          TT      HH     HH  IIIIIIIII  SSSSS
  }
}

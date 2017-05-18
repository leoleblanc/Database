package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.RecordID;

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
    //TODO: Implement Me!!
    if (findFirst) {
        Iterator<RecordID> rids = scanForKey(key);
        if (!rids.hasNext()) {
            return this;
        }
        LeafNode leaf = this;
        LeafNode currLeaf = this;
        while (currLeaf.getPrevLeaf() != -1) {
            if (currLeaf.scanForKey(key).hasNext()) {
                leaf = currLeaf;
            } else {
                break;
            }
            currLeaf = (LeafNode) getBPlusNode(this.getTree(), currLeaf.getPrevLeaf());
        }
        if (currLeaf.scanForKey(key).hasNext()) {
            return currLeaf;
        } else {
            return leaf;
        }
    } else {
        Iterator<RecordID> rids = scanForKey(key);
        if (!rids.hasNext()) {
            return this;
        }
        LeafNode leaf = this;
        LeafNode currLeaf = this;
        while (currLeaf.getNextLeaf() != -1) {
            if (currLeaf.scanForKey(key).hasNext()) {
                leaf = currLeaf;
            } else {
                break;
            }
            currLeaf = (LeafNode) getBPlusNode(this.getTree(), currLeaf.getNextLeaf());
        }
        if (currLeaf.scanForKey(key).hasNext()) {
            return currLeaf;
        } else {
            return leaf;
        }
    }
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
    //TODO: Implement Me!!
      int oldNextLeaf = this.getNextLeaf();
      LeafNode node = new LeafNode(this.getTree());
      List<BEntry> entries = this.getAllValidEntries();
      List<BEntry> curr = entries.subList(0, numEntries/2);
      List<BEntry> other = entries.subList(numEntries/2, numEntries);

      BEntry push = other.get(0);
      InnerEntry newEnt = new InnerEntry(push.getKey(), node.getPageNum()); //make this point to new leaf

      this.overwriteBNodeEntries(curr); //overwrite this node's entries
      node.overwriteBNodeEntries(other); //put the entries in the new node
      int parentPage = this.getParent();
      InnerNode parentNode;

      if (parentPage == -1) { //if it has no parent, it is Root
          parentNode = new InnerNode(this.getTree());
          parentPage = parentNode.getPageNum();
          this.getTree().updateRoot(parentPage);
          parentNode.setFirstChild(this.getPageNum());
      } else {
          parentNode = (InnerNode) getBPlusNode(this.getTree(), parentPage);
      }

      this.setParent(parentPage);
      node.setParent(parentPage);
      this.setNextLeaf(node.getPageNum());
      node.setPrevLeaf(this.getPageNum());

      if (oldNextLeaf != -1) {
          LeafNode oldNextLeafNode = (LeafNode) BPlusNode.getBPlusNode(this.getTree(), oldNextLeaf);
          oldNextLeafNode.setPrevLeaf(node.getPageNum());
      }
      node.setNextLeaf(oldNextLeaf);
      parentNode.insertBEntry(newEnt);

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

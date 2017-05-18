package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;

import java.util.ArrayList;
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
    //TODO: Implement Me!!
    List<BEntry> entries = this.getAllValidEntries();
    int goTo = 0;
    for (int count = 0; count < entries.size(); count++) {
        if (count == 0) {
            if (key.compareTo(entries.get(count).getKey()) < 0) {
                goTo = this.getFirstChild();
                break;
            }
        }
        if (count + 1 == entries.size()) { //there is no next entry
            goTo = entries.get(count).getPageNum();
            break;
        } else {
            if ((key.compareTo(entries.get(count).getKey()) >= 0) & (key.compareTo(entries.get(count+1).getKey()) < 0)) {
//            if (key.compareTo(entries.get(count+1).getKey()) <= 0) {
                goTo = entries.get(count).getPageNum();
                break;
            }
        }
    }
    BPlusNode node = getBPlusNode(this.getTree(), goTo);
    return node.locateLeaf(key, findFirst);
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
    //TODO: Implement me!!
    boolean musty = true;
    boolean crusty = true;
    boolean dusty = true;
    boolean rusty = true;
    if (musty & dusty & crusty & rusty) {
        InnerNode newNode = new InnerNode(this.getTree());
        int parentPage = this.getParent();
        InnerNode parentNode;
        if (isRoot()) {
            parentNode = new InnerNode(this.getTree());
            this.getTree().updateRoot(parentNode.getPageNum());
            parentNode.setFirstChild(this.getPageNum());
            this.setParent(parentNode.getPageNum());
        } else {
            parentNode = (InnerNode) getBPlusNode(this.getTree(), parentPage);
        }

        List<BEntry> entries = this.getAllValidEntries();
        List<BEntry> curr = entries.subList(0, numEntries/2);
        List<BEntry> other = entries.subList(numEntries/2+1, numEntries);

        BEntry push = entries.get(numEntries/2);
        int pushChildPage = push.getPageNum();
        BPlusNode pushChildNode = BPlusNode.getBPlusNode(this.getTree(), pushChildPage);
        pushChildNode.setParent(newNode.getPageNum());
        newNode.setFirstChild(pushChildPage);

        InnerEntry newEnt = new InnerEntry(push.getKey(), newNode.getPageNum());

        this.overwriteBNodeEntries(curr);
        newNode.overwriteBNodeEntries(other);
//        this.setParent(parentNode.getPageNum());
        newNode.setParent(parentNode.getPageNum());

        for (BEntry bEntry: newNode.getAllValidEntries()) {
            BPlusNode node = BPlusNode.getBPlusNode(this.getTree(), bEntry.getPageNum());
            node.setParent(newNode.getPageNum());
        }
        parentNode.insertBEntry(newEnt);
    }
  }
}

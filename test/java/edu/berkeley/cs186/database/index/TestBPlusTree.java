package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.table.RecordID;
import edu.berkeley.cs186.database.datatypes.*;
import edu.berkeley.cs186.database.StudentTest;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runners.MethodSorters;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.Arrays;
import java.util.Random;
import static org.junit.Assert.*;

public class TestBPlusTree {
  public static final String testFile = "BPlusTreeTest";
  private BPlusTree bp;
  public static final int intLeafPageSize = 400;
  public static final int intInnPageSize = 496;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  
  @Rule
  public Timeout globalTimeout = Timeout.seconds(80); // 80 seconds max per method tested

  @Before
  public void beforeEach() throws Exception {
    tempFolder.newFile(testFile);
    String tempFolderPath = tempFolder.getRoot().getAbsolutePath();
    this.bp = new BPlusTree(new IntDataType(), testFile, tempFolderPath);
  }

  @Test
  public void testBPlusTreeInsert() {
//      System.out.println("testing BPlusTreeInsert");

    for (int i = 0; i < 10; i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }

    Iterator<RecordID> rids = bp.sortedScan();
    int count = 0;
    while (rids.hasNext()) {
      assertEquals(count, rids.next().getPageNum());
      count++;
    }
//      System.out.println("testing BPlusTreeInsert: passed");
  }
  @Test
  public void testBPlusTreeInsertBackwards() {
//      System.out.println("testing BPlusTreeInsertBackwards");
    for (int i = 9; i >= 0; i--) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    int count = 0;
    while (rids.hasNext()) {
      assertEquals(count, rids.next().getPageNum());
      count++;
    }
//      System.out.println("testing BPlusTreeInsertBackwards passed");
  }

  @Test
  public void testBPlusTreeInsertIterateFrom() {
//      System.out.println("testing BPlusTreeInsertIterateFrom");
    for (int i = 16; i >= 0; i--) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }
    Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataType(10));
    int count = 10;
    while (rids.hasNext()) {
      assertEquals(count, rids.next().getPageNum());
      count++;
    }
    assertEquals(17, count);
//      System.out.println("testing BPlusTreeInsertIterateFrom: passed");
  }

  @Test
  public void testBPlusTreeInsertIterateFromDuplicate() {
//      System.out.println("testing BPlusTreeInsertIterateFromDuplicate");
    for (int i = 10; i >= 0; i--) {
      for (int j = 0; j < 8; j++) {
        bp.insertKey(new IntDataType(i), new RecordID(i,j));
      }
    }
    Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataType(5));
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(5 + counter/8, rid.getPageNum());
      assertEquals(counter % 8, rid.getSlotNumber());
      counter++;
    }
    assertEquals((5+1)*8, counter);
//      System.out.println("testing BPlusTreeInsertIterateFromDuplicate: passed");
  }

  @Test
  public void testBPlusTreeInsertIterateLookup() {
//      System.out.println("testing BPlusTreeInsertIterateLookup");
    for (int i = 10; i >= 0; i--) {
      for (int j = 0; j < 8; j++) {
        bp.insertKey(new IntDataType(i), new RecordID(i,j));
      }
    }
    Iterator<RecordID> rids = bp.lookupKey(new IntDataType(5));
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(5, rid.getPageNum());
      assertEquals(counter, rid.getSlotNumber());
      counter++;
    }
    assertEquals(8, counter);
//      System.out.println("testing BPlusTreeInsertIterateLookup: passed");
  }

  @Test
  public void testBPlusTreeInsertIterateFullLeafNode() {
//      System.out.println("testing BPlusTreeInsertIterateFullLeafNode");
    for (int i = 0; i < 400; i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(counter, rid.getPageNum());
      counter++;
    }
    assertEquals(400, counter);
//      System.out.println("testing BPlusTreeInsertIterateFullLeafNode: passed");
  }

  @Test
  public void testBPlusTreeInsertIterateFullLeafSplit() {
//        System.out.print("testing InsertIterateFullLeafSplit");
    //Insert full leaf of records + 1
    for (int i = 0; i < intLeafPageSize + 1; i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }

    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(counter, rid.getPageNum());
      counter++;
    }
    assertEquals(intLeafPageSize + 1, counter);
//      System.out.println(": passed");
  }

  @Test
  public void testBPlusTreeInsertAppendIterateMultipleFullLeafSplit() {
//      System.out.print("starting InsertAppendIterateMultipleFullLeafSplit");
    //Insert 3 full leafs of records + 1 in append fashion
    for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i,0));
    }

    Iterator<RecordID> rids = bp.sortedScan();
    int counter = 0;
    while (rids.hasNext()) {
      RecordID rid = rids.next();
      assertEquals(counter, rid.getPageNum());
      counter++;
    }
    assertEquals(3*intLeafPageSize + 1, counter);
//      System.out.println(": passed ");
  }

  @Test
  public void testBPlusTreeSweepInsertSortedScanMultipleFullLeafSplit() {
//      System.out.print("starting SweepInsertSortedScanMultipleFullLeafSplit");
    //Insert 3 full leafs of records + 1 in sweeping fashion
    for (int i = 0; i < 3*intLeafPageSize + 1; i++) {
      bp.insertKey(new IntDataType(i % 3), new RecordID(i % 3, i));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());

    for (int i = 0; i < intLeafPageSize + 1; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(0, rid.getPageNum());
    }

    for (int i = 0; i < intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(1, rid.getPageNum());
    }

    for (int i = 0; i < intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(2, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
//      System.out.println(": passed");
  }

  @Test
  public void testBPlusTreeRandomInsertSortedScanLeafSplit() {
//      System.out.print("starting RandomInsertSortedScanLeafSplit");
    Random rand = new Random(0); //const seed
    for (int i = 0; i < 10*intLeafPageSize; i++) {
      int val = rand.nextInt();
      bp.insertKey(new IntDataType(val), new RecordID(val, 0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    int last = rids.next().getPageNum();
    for (int i = 0; i < 10*intLeafPageSize - 1; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertTrue(last + " not less than " + rid.getPageNum(), last <= rid.getPageNum());
      last = rid.getPageNum();
    }
    assertFalse(rids.hasNext());
//      System.out.println(": passed");
  }

  @Test
  public void testBPlusTreeSweepInsertLookupKeyMultipleFullLeafSplit() {
//      System.out.print("starting SweepInsertLookupKeyMultipleFullLeafSplit");
    //Insert 4 full leafs of records in sweeping fashion
    for (int i = 0; i < 8*intLeafPageSize; i++) {
      bp.insertKey(new IntDataType(i % 4), new RecordID(i % 4, i));
    }
    Iterator<RecordID> rids = bp.lookupKey(new IntDataType(0));
    assertTrue(rids.hasNext());

    for (int i = 0; i < 2*intLeafPageSize; i++) {
      assertTrue("iteration " + i, rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(0, rid.getPageNum());
    }
    assertFalse(rids.hasNext());

    rids = bp.lookupKey(new IntDataType(1));
    assertTrue(rids.hasNext());
    for (int i = 0; i < 2*intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(1, rid.getPageNum());
    }
    assertFalse(rids.hasNext());

    rids = bp.lookupKey(new IntDataType(2));
    assertTrue(rids.hasNext());

    for (int i = 0; i < 2*intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(2, rid.getPageNum());
    }
    assertFalse(rids.hasNext());

    rids = bp.lookupKey(new IntDataType(3));
    assertTrue(rids.hasNext());

    for (int i = 0; i < 2*intLeafPageSize; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(3, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
//        System.out.println(": passed");
  }

  @Test
  public void testBPlusTreeSweepInsertSortedScanLeafSplit() {
//      System.out.println("starting SweepInsertSortedScanLeafSplit");
    //Insert 10 full leafs of records in sweeping fashion
    for (int i = 0; i < 10*intLeafPageSize; i++) {
      bp.insertKey(new IntDataType(i % 5), new RecordID(i % 5, i));
    }

    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 2*intLeafPageSize; j++) {
        assertTrue(rids.hasNext());
        RecordID rid = rids.next();
        assertEquals(i, rid.getPageNum());
      }
    }
    assertFalse(rids.hasNext());
//      System.out.println(": passed");
  }

  @Test
  public void testBPlusTreeSweepInsertSortedScanFromLeafSplit() {
//      System.out.println("starting SweepInsertSortedScanFromLeafSplit");
    //Insert 10 full leafs of records in sweeping fashion
    for (int i = 0; i < 10*intLeafPageSize; i++) {
      bp.insertKey(new IntDataType(i % 5), new RecordID(i % 5, i));
    }
    for (int k = 0; k < 5; k++) {
      Iterator<RecordID> rids = bp.sortedScanFrom(new IntDataType(k));
      assertTrue(rids.hasNext());
      for (int i = k; i < 5; i++) {
        for (int j = 0; j < 2*intLeafPageSize; j++) {
          assertTrue(rids.hasNext());
          RecordID rid = rids.next();
          assertEquals(i, rid.getPageNum());
        }
      }
      assertFalse(rids.hasNext());
//        System.out.println(": passed");
    }
  }

  @Test
  public void testBPlusTreeAppendInsertSortedScanInnerSplit() {
//      System.out.println("starting AppendInsertSortedScanInnerSplit");
    //insert enough for InnerNode Split
    for (int i = 0; i < (intInnPageSize/2 + 1)*(intLeafPageSize); i++) {
      bp.insertKey(new IntDataType(i), new RecordID(i, 0));
    }
    Iterator<RecordID> rids = bp.sortedScan();

    for (int i = 0; i < (intInnPageSize/2 + 1)*(intLeafPageSize); i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertEquals(i, rid.getPageNum());
    }
    assertFalse(rids.hasNext());
//      System.out.println(": passed");
  }

  @Test
  public void testBPlusTreeSweepInsertLookupInnerSplit() {
    //insert enough for InnerNode Split; numEntries + firstChild
    //each key should span 2 pages
//      System.out.println("starting SweepInsertLookupInnerSplit");
    for (int i = 0; i < 2*intLeafPageSize; i++) {
      for (int k = 0; k < 250; k++) {
        bp.insertKey(new IntDataType(k), new RecordID(k, 0));
      }
    }

    for (int k = 0; k < 250; k++) {
      Iterator<RecordID> rids = bp.lookupKey(new IntDataType(k));
      for (int i = 0; i < 2*intLeafPageSize; i++) {
        assertTrue("Loop: " + k + " iteration " + i, rids.hasNext());
        RecordID rid = rids.next();
        assertEquals(k, rid.getPageNum());
      }
      assertFalse(rids.hasNext());
    }
  }
  @Test
  public void testBPlusTreeRandomInsertSortedScanInnerSplit() {
    //insert enough for InnerNode Split; numEntries + firstChild
    Random rand = new Random(0); //const seed
    int innerNodeSplit = intInnPageSize;

    for (int i = 0; i < innerNodeSplit*intLeafPageSize; i++) {
      int val = rand.nextInt();
      bp.insertKey(new IntDataType(val), new RecordID(val, 0));
    }
    Iterator<RecordID> rids = bp.sortedScan();
    assertTrue(rids.hasNext());
    int last = rids.next().getPageNum();
    for (int i = 0; i < innerNodeSplit*intLeafPageSize - 1; i++) {
      assertTrue(rids.hasNext());
      RecordID rid = rids.next();
      assertTrue("iteration: " + i + " last: " + last + " curr: " + rid.getPageNum(), last <= rid.getPageNum());
      last = rid.getPageNum();
    }
    assertFalse(rids.hasNext());
  }

  @Test
  @Category(StudentTest.class) public void testOneLeafSplit() {
      for (int i = 0; i < intLeafPageSize; i++) { //split, create new root
          bp.insertKey(new IntDataType(i), new RecordID(i, 0));
          assertTrue(bp.containsKey(new IntDataType(i))); //this checks to see that leaf locate works
      }
      bp.insertKey(new IntDataType(401), new RecordID(401, 0));
      assertTrue(bp.containsKey(new IntDataType(401)));
  }

  @Test
  @Category(StudentTest.class) public void testMultipleLeafSplits() {
      for (int i = 0; i < intLeafPageSize*2; i++) { //2 splits
          bp.insertKey(new IntDataType(i), new RecordID(i, 0));
          assertTrue(bp.containsKey(new IntDataType(i)));
      }
      bp.insertKey(new IntDataType(1000), new RecordID(1000, 0));
      assertTrue(bp.containsKey(new IntDataType(1000)));
  }

  @Test
  @Category(StudentTest.class) public void testSoManySplitsInsertLookupBackwards() {
      int countInsertForwards = 0;
      int countLookupBackwards = 0;
      for (int i = 0; i < intLeafPageSize*intInnPageSize; i++, countInsertForwards++) {
          bp.insertKey(new IntDataType(i), new RecordID(i, 0));
//          assertTrue(bp.containsKey(new IntDataType(i)));
      }
      for (int i = intLeafPageSize*intInnPageSize-1; i >= 0; i--, countLookupBackwards++) {
          assertTrue(bp.containsKey(new IntDataType(i)));
      }
      assertTrue(countInsertForwards == countLookupBackwards);

  }

  @Test
  @Category(StudentTest.class) public void testOneDiffKeyTreeInsertLeftInnerSplit() {
      for (int i = 0; i < intLeafPageSize*intInnPageSize; i++) {
          bp.insertKey(new IntDataType(0), new RecordID(2, 0));
      }
      bp.insertKey(new IntDataType(-1), new RecordID(42, 0));
      assertTrue(bp.containsKey(new IntDataType(-1)));
  }

  @Test
  @Category(StudentTest.class) public void testOneDiffKeyTreeInsertRightInnerSplit() {
      for (int i = 0; i < intLeafPageSize*intInnPageSize; i++) {
          bp.insertKey(new IntDataType(0), new RecordID(2, 0));
      }
      bp.insertKey(new IntDataType(1), new RecordID(42, 0));
      assertTrue(bp.containsKey(new IntDataType(1)));
  }

  @Test
  @Category(StudentTest.class) public void testInvalidLookups() {
      for (int i = 0; i < intLeafPageSize; i++) {
          bp.insertKey(new IntDataType(i), new RecordID(i, 0));
      }

      for (int i = intLeafPageSize; i < intLeafPageSize*2; i++) {
          assertFalse(bp.containsKey(new IntDataType(i)));
      }
  }
}


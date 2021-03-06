package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.*;
import edu.berkeley.cs186.database.table.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.Runnable;

public class TestLockManager {
  private static final String TestDir = "testDatabase";
  private Database db;
  private String filename;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  
  @Rule
  public Timeout maxGlobalTimeout = Timeout.seconds(10); // 10 seconds max per method tested


  @Before
  public void beforeEach() throws IOException, DatabaseException {
    File testDir = tempFolder.newFolder(TestDir);
    this.filename = testDir.getAbsolutePath();
    this.db = new Database(filename);
    this.db.deleteAllTables();
  }

  @After
  public void afterEach() {
    this.db.deleteAllTables();
    this.db.close();
  }

  @Test
  public void testTransaction() throws DatabaseException {
    String tableName = "testTable1";
    Schema s = TestUtils.createSchemaWithAllTypes();
    db.createTable(s, tableName); 
    Record input = TestUtils.createRecordWithAllTypes();
    
    Database.Transaction t1 = db.beginTransaction();
    RecordID rid = t1.addRecord(tableName, input.getValues());
    Record rec = t1.getRecord(tableName, rid);
    assertEquals(input, rec);
    t1.end();  
  }
  
  @Test 
  public void testTwoTableTwoTransactions() throws DatabaseException, InterruptedException {
    final String tableName1 = "testTable1";
    final String tableName2 = "testTable2";
    
    Schema s = TestUtils.createSchemaWithAllTypes();
    db.createTable(s, tableName1);
    db.createTable(s, tableName2);

    final Record input = TestUtils.createRecordWithAllTypes();
    final Database.Transaction t1 = db.beginTransaction();
    final Database.Transaction t2 = db.beginTransaction();
    
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        try {
          t1.addRecord(tableName1, input.getValues());
        } catch (DatabaseException e) {
          System.out.println(e.getMessage());
        }
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        try {
          t2.addRecord(tableName2, input.getValues());
        } catch (DatabaseException e) {
          System.out.println(e.getMessage());
        }
      }
    }, "Transaction 2 Thread");
    
    thread1.start();
    thread1.join(1000); //waits for thread to finish (timeout of 1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive());

    thread2.start();
    thread2.join(1000); //waits for thread to finish (timeout of 1 sec)
    assertFalse("Transaction 2 Thread should have finished", thread2.isAlive());
    
    t1.end();
    t2.end();
  }

  @Test
  public void testTwoSharedSameTransactionLockManager() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Second Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should have shared lock

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Second Thread should have finished", thread2.isAlive()); //T1 Second thread should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); // T1 should still have shared lock

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should now not have lock
  }

  @Test
  public void testTwoSharedDifferentTransactionLockManager() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should have shared lock
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.SHARED));

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be waiting on T1

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should have shared lock
    assertTrue(lockMan.holdsLock("A", 2, LockManager.LockType.SHARED)); //T2 should also have shared lock

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 now does not have shared lock

    lockMan.releaseLock("A", 2); //Transaction 2 releasing its lock

    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.SHARED)); //T2 now does not have shared lock
  }

  @Test
  public void testSharedAndExclusiveDifferentTransactionsLockManager() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should have shared lock
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE));

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1

    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); //T2 still should not have exclusive lock

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock

    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked anymore

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED));
    assertTrue(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); //T2 should now have exclusive lock

    lockMan.releaseLock("A", 2); //Transaction 2 releasing its lock

    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); //T2 should now not have exclusive lock
  }

  @Test
  public void testSharedAndExclusiveLockUpgrade1() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    Thread thread3 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should have shared lock

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should still have shared lock
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); //T2 did not obtain exclusive lock

    thread3.start();
    thread3.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Second Thread should have finished", thread3.isAlive()); //T1 Second thread should not be blocked

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED));
    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); //T1 should now have exclusive lock
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); //T2 still did not obtain exclusive lock

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock
    lockMan.releaseLock("A", 2); //Transaction 2 releasing its lock
  }

  @Test
  public void testSharedAndExclusiveLockUpgrade2() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    Thread thread3 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should have shared lock

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should still have shared lock
    assertTrue(lockMan.holdsLock("A", 2, LockManager.LockType.SHARED)); //T2 should now have shared lock

    thread3.start();
    thread3.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 1 Second Thread should not have finished", thread3.isAlive()); //T1 Second thread should be blocked

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); // T1 should not have upgraded to exclusive lock

    lockMan.releaseLock("A", 2); //Transaction 2 releasing its lock

    thread3.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Second Thread should have finished", thread3.isAlive()); //T1 should not be blocked

    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.SHARED)); //T2 now should not have shared lock
    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.SHARED)); //T1 should now have exclusive lock (lock upgrade)
    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); //T1 should now not have exclusive lock
  }

  @Test
  public void testTwoExclusiveSameTransactionLockManager() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); //T1 should have exclusive lock

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Second Thread should have finished", thread2.isAlive()); //T1 Second Thread should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); //T1 should still have exclusive lock

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); //T1 now should not have exclusive lock
  }

  @Test
  public void testTwoExclusiveDifferentTransactionLockManager() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); //T1 should have exclusive lock
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE));

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 2 Thread should still be running", thread2.isAlive()); //T2 should be waiting on T1

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); // T2 still should not have exclusive lock

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock

    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
    assertTrue(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); //T2 should now have exclusive lock

    lockMan.releaseLock("A", 2); //Transaction 2 releasing its lock

    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); //T2 should now not have exclusive lock
  }

  @Test
  public void testFIFOQueueLocks() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    Thread thread3 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    Thread thread4 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 4, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 4 Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 2 Thread should still be running", thread2.isAlive()); //T2 should be waiting on T1

    thread3.start();
    thread3.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 3 Thread should still be running", thread3.isAlive()); //T3 should be waiting on T1

    thread4.start();
    thread4.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 4 Thread should still be running", thread4.isAlive()); //T4 should be waiting on T1

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); //T1 should have exclusive lock
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 3, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 4, LockManager.LockType.EXCLUSIVE));

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock

    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    thread3.join(100);
    thread4.join(100);
    assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked
    assertTrue("Transaction 3 Thread should have finished", thread3.isAlive()); //T3 should be blocked
    assertTrue("Transaction 4 Thread should have finished", thread4.isAlive()); //T4 should be blocked

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
    assertTrue(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE)); //T2 should have exclusive lock
    assertFalse(lockMan.holdsLock("A", 3, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 4, LockManager.LockType.EXCLUSIVE));

    lockMan.releaseLock("A", 2); //Transaction 2 releasing its lock

    thread3.join(100); //waits for thread to finish (timeout of .1 sec)
    thread4.join(100);
    assertFalse("Transaction 3 Thread should have finished", thread3.isAlive()); //T3 should not be blocked
    assertTrue("Transaction 4 Thread should have finished", thread4.isAlive()); //T4 should be blocked

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE));
    assertTrue(lockMan.holdsLock("A", 3, LockManager.LockType.EXCLUSIVE)); //T3 should have exclusive lock
    assertFalse(lockMan.holdsLock("A", 4, LockManager.LockType.EXCLUSIVE));

    lockMan.releaseLock("A", 3); //Transaction 3 releasing its lock

    thread4.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 4 Thread should have finished", thread4.isAlive()); //T4 should not be blocked

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 3, LockManager.LockType.EXCLUSIVE));
    assertTrue(lockMan.holdsLock("A", 4, LockManager.LockType.EXCLUSIVE)); //T4 should have exclusive lock

    lockMan.releaseLock("A", 4); //Transaction 4 releasing its lock

    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); // All transactions have released the lock
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 3, LockManager.LockType.EXCLUSIVE));
    assertFalse(lockMan.holdsLock("A", 4, LockManager.LockType.EXCLUSIVE));
  }

  @Test
  public void testManySharedPromoteSimultaneousLockManager() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    Thread thread3 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 3, LockManager.LockType.SHARED);
      }
    }, "Transaction 3 Thread");

    Thread thread4 = new Thread(new Runnable() {
      public void run() {
        /*Code to run goes inside here */
        lockMan.acquireLock("A", 4, LockManager.LockType.SHARED);
      }
    }, "Transaction 4 Thread");

    thread1.start();
    thread1.join(100); //waits for thread to finish (timeout of .1 sec)
    assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

    thread2.start();
    thread2.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1

    thread3.start();
    thread3.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 3 Thread should not have finished", thread3.isAlive()); //T3 should be waiting on T1

    thread4.start();
    thread4.join(100); //waits for thread to finish (timeout of .1 sec)
    assertTrue("Transaction 4 Thread should not have finished", thread4.isAlive()); //T4 should be waiting on T1

    assertTrue(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE)); //T1 has exclusive lock on A
    assertFalse(lockMan.holdsLock("A", 2, LockManager.LockType.SHARED));
    assertFalse(lockMan.holdsLock("A", 3, LockManager.LockType.SHARED));
    assertFalse(lockMan.holdsLock("A", 4, LockManager.LockType.SHARED));

    lockMan.releaseLock("A", 1); //Transaction 1 releasing its lock

    thread2.join(100); //wait for thread 2 to finish
    assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

    thread3.join(100); //wait for thread 3 to finish
    assertFalse("Transaction 3 Thread should have finished", thread3.isAlive()); //T3 should not be blocked

    thread4.join(100); //wait for thread 4 to finish
    assertFalse("Transaction 4 Thread should have finished", thread4.isAlive()); //T4 should not be blocked

    //T2,T3,T4 all promoted from queue and now have shared locks on A
    assertFalse(lockMan.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
    assertTrue(lockMan.holdsLock("A", 2, LockManager.LockType.SHARED));
    assertTrue(lockMan.holdsLock("A", 3, LockManager.LockType.SHARED));
    assertTrue(lockMan.holdsLock("A", 4, LockManager.LockType.SHARED));
  }

  /**
   * Test sample, do not modify.
   */
  @Test
  @Category(StudentTestP3.class)
  public void testSample() {
    assertEquals(true, true); // Do not actually write a test like this!
  }


  @Test
  @Category(StudentTestP3.class)
  public void testSimpleUpgrade() throws InterruptedException {
    final LockManager manager = new LockManager();
    Thread thread1 = new Thread(new Runnable() {
      public void run() {
          manager.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    Thread thread2 = new Thread(new Runnable() {
        public void run() {
            manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
        }
    }, "Transaction 1 Thread 2");

      thread1.start();
      thread1.join(100);
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive());

      assertTrue(manager.holdsLock("A", 1, LockManager.LockType.SHARED));

      thread2.start();
      thread2.join(100);
      assertFalse("Transaction 1 Thread 2 should have finished", thread2.isAlive());

      assertTrue(manager.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));

  }

  @Test
  @Category(StudentTestP3.class)
  public void testUpgradeAfterWait() throws InterruptedException {
      final LockManager manager = new LockManager();
      Thread thread1 = new Thread(new Runnable() {
          public void run() {
              manager.acquireLock("A", 1, LockManager.LockType.SHARED);
          }
      }, "Transaction 1 Thread");

      Thread thread12 = new Thread(new Runnable() {
          public void run() {
              manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
          }
      }, "Transaction 1 Thread 2");

      Thread thread2 = new Thread(new Runnable() {
          public void run() {
              manager.acquireLock("A", 2, LockManager.LockType.SHARED);
          }
      }, "Transaction 2 Thread");

      Thread thread22 = new Thread(new Runnable() {
          public void run() {
              manager.releaseLock("A", 2);
//              manager.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
          }
      }, "Transaction 2 Thread 2");

      Thread thread3 = new Thread(new Runnable() {
          public void run() {
              manager.acquireLock("A", 3, LockManager.LockType.SHARED);
          }
      }, "Transaction 3 Thread");

      Thread thread32 = new Thread(new Runnable() {
          public void run() {
              manager.releaseLock("A", 3);
//              manager.acquireLock("A", 3, LockManager.LockType.EXCLUSIVE);
          }
      }, "Transaction 3 Thread 2");

      Thread thread4 = new Thread(new Runnable() {
          public void run() {
              manager.acquireLock("A", 4, LockManager.LockType.SHARED);
          }
      }, "Transaction 4 Thread");

      Thread thread42 = new Thread(new Runnable() {
          public void run() {
              manager.releaseLock("A", 4);
//              manager.acquireLock("A", 4, LockManager.LockType.EXCLUSIVE);
          }
      }, "Transaction 4 Thread 2");

      thread1.start();
      thread1.join(100);
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive());

      thread2.start();
      thread2.join(100);
      assertFalse("Transaction 2 Thread should have finished", thread2.isAlive());

      thread3.start();
      thread3.join(100);
      assertFalse("Transaction 3 Thread should have finished", thread1.isAlive());

      thread4.start();
      thread4.join(100);
      assertFalse("Transaction 4 Thread should have finished", thread1.isAlive());

      thread12.start();
      thread12.join(100);
      assertTrue("Transaction 1 Thread 2 should not have finished", thread12.isAlive()); //still waiting

      //release the shared locks
      thread22.start();
      thread22.join(100);
      assertFalse("Transaction 2 Thread 2 should have finished", thread22.isAlive());

      assertFalse(manager.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));

      thread32.start();
      thread32.join(100);
      assertFalse("Transaction 3 Thread 2 should have finished", thread32.isAlive());

      assertFalse(manager.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));

      thread42.start();
      thread42.join(100);
      assertFalse("Transaction 4 Thread 2 should have finished", thread42.isAlive());

      //at this point, T1 should now have exclusive lock
      assertTrue(manager.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
  }

    @Test
    @Category(StudentTestP3.class)
    public void testOnlySharedNoBlocks() throws InterruptedException {
        final LockManager manager = new LockManager();
        Thread thread1 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.SHARED);
            }
        }, "Transaction 1 Thread");

        Thread thread2 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 2, LockManager.LockType.SHARED);
            }
        }, "Transaction 2 Thread");

        Thread thread3 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 3, LockManager.LockType.SHARED);
            }
        }, "Transaction 3 Thread");

        Thread thread4 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 4, LockManager.LockType.SHARED);
            }
        }, "Transaction 4 Thread");

        Thread thread5 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 5, LockManager.LockType.SHARED);
            }
        }, "Transaction 5 Thread");

        Thread thread6 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 6, LockManager.LockType.SHARED);
            }
        }, "Transaction 6 Thread");

        thread1.start();
        thread1.join(100);
        thread2.start();
        thread2.join(100);
        thread3.start();
        thread3.join(100);
        thread4.start();
        thread4.join(100);
        thread5.start();
        thread5.join(100);
        thread6.start();
        thread6.join(100);

        assertFalse(thread1.isAlive());
        assertFalse(thread2.isAlive());
        assertFalse(thread3.isAlive());
        assertFalse(thread4.isAlive());
        assertFalse(thread5.isAlive());
        assertFalse(thread6.isAlive());

        assertTrue(manager.holdsLock("A", 1, LockManager.LockType.SHARED));
        assertTrue(manager.holdsLock("A", 2, LockManager.LockType.SHARED));
        assertTrue(manager.holdsLock("A", 3, LockManager.LockType.SHARED));
        assertTrue(manager.holdsLock("A", 4, LockManager.LockType.SHARED));
        assertTrue(manager.holdsLock("A", 5, LockManager.LockType.SHARED));
        assertTrue(manager.holdsLock("A", 6, LockManager.LockType.SHARED));
    }


    @Test
    @Category(StudentTestP3.class)
    public void testPrioritizeUpgrades() throws InterruptedException {
        final LockManager manager = new LockManager();
        Thread thread1 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.SHARED);
            }
        }, "Transaction 1 Thread");

        Thread thread12 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 1 Thread 2");

        Thread thread2 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 2 Thread");

        Thread thread3 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 3, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 3 Thread");

        thread1.start();
        thread1.join(100);

        thread2.start();
        thread2.join(100);
        thread3.start();
        thread3.join(100);

        assertTrue(thread2.isAlive());
        assertTrue(thread3.isAlive());

        thread12.start();
        thread12.join(100);

        assertFalse(thread12.isAlive());
        assertTrue(thread2.isAlive());
        assertTrue(thread3.isAlive());

        assertTrue(manager.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
        assertFalse(manager.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE));
        assertFalse(manager.holdsLock("A", 3, LockManager.LockType.EXCLUSIVE));
    }

    @Test
    @Category(StudentTestP3.class)
    public void testNoWaitOnSelf() throws InterruptedException {
        final LockManager manager = new LockManager();
        Thread thread1 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 1 Thread");

        Thread thread12 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 1 Thread2");

        Thread thread13 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 1 Thread3");

        Thread thread14 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 1 Thread2");

        thread1.start();
        thread1.join(100);

        thread12.start();
        thread12.join(100);
        thread13.start();
        thread13.join(100);
        thread14.start();
        thread14.join(100);

        assertFalse(thread1.isAlive());
        assertFalse(thread12.isAlive());
        assertFalse(thread13.isAlive());
        assertFalse(thread14.isAlive());

        assertTrue(manager.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
    }

    @Test
    @Category(StudentTestP3.class)
    public void testOnlyOneExclusivePossession() throws InterruptedException {
        final LockManager manager = new LockManager();
        Thread thread1 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 1 Thread");

        Thread thread2 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 2 Thread");

        Thread thread3 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 3, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 3 Thread");

        Thread thread4 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 4, LockManager.LockType.SHARED);
            }
        }, "Transaction 4 Thread");

        thread1.start();
        thread1.join(100);

        thread2.start();
        thread2.join(100);
        thread3.start();
        thread3.join(100);
        thread4.start();
        thread4.join(100);

        assertFalse(thread1.isAlive());
        assertTrue(thread2.isAlive());
        assertTrue(thread3.isAlive());
        assertTrue(thread4.isAlive());

        assertTrue(manager.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));
        assertFalse(manager.holdsLock("A", 2, LockManager.LockType.EXCLUSIVE));
        assertFalse(manager.holdsLock("A", 3, LockManager.LockType.EXCLUSIVE));
        assertFalse(manager.holdsLock("A", 4, LockManager.LockType.SHARED));
    }

    @Test
    @Category(StudentTestP3.class)
    public void noDowngrades() throws InterruptedException {
        final LockManager manager = new LockManager();
        Thread thread1 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
            }
        }, "Transaction 1 Thread");

        Thread thread12 = new Thread(new Runnable() {
            public void run() {
                manager.acquireLock("A", 1, LockManager.LockType.SHARED);
            }
        }, "Transaction 1 Thread 2");

        thread1.start();
        thread1.join(100);

        thread12.start();
        thread12.join(100);

        assertFalse(thread1.isAlive());
        assertFalse(thread12.isAlive());

        assertFalse(manager.holdsLock("A", 1, LockManager.LockType.SHARED));
        assertTrue(manager.holdsLock("A", 1, LockManager.LockType.EXCLUSIVE));

    }


}


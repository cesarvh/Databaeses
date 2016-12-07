package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.StudentTestP3;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestDeadlockPrevention {
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
  public void testNoCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertTrue("Transaction 3 Thread should not have finished", thread4.isAlive()); //T3 should be blocked on B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  @Test
  public void testNoDirectedCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertTrue("Transaction 3 Thread should not have finished", thread3.isAlive()); //T3 should be waiting on T1 for A

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread4.isAlive()); //T2 should not be blocked on B

      thread5.start();
      thread5.join(100);
      thread5.test();
      assertTrue("Transaction 3 Second Thread should not have finished", thread5.isAlive()); //T3 should be waiting on T2 for B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }

  @Test
  public void testTwoTransactionCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread4.start();
      thread4.join(100);
      thread4.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testThreeTransactionCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    AsyncDeadlockTesterThread thread6 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread3.isAlive()); //T2 should not be blocked on B

      thread4.start();
      thread4.join(100);
      thread4.test();
      assertTrue("Transaction 3 Thread should not have finished", thread4.isAlive()); //T3 should be blocked on B

      thread5.start();
      thread5.join(100);
      thread5.test();
      assertFalse("Transaction 3 Second Thread should have finished", thread5.isAlive()); //T3 should not be blocked on C
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread6.start();
      thread6.join(100);
      thread6.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testThreeTransactionCycleDeadlock2() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");

    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Third Thread");

    AsyncDeadlockTesterThread thread6 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread7 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("C", 3, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 3 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked on A

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertTrue("Transaction 2 Thread should not have finished", thread2.isAlive()); //T2 should be waiting on T1 for A

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertFalse("Transaction 3 Thread should have finished", thread3.isAlive()); //T3 should not be blocked on B

      thread4.start();
      thread4.join(100); //waits for thread to finish (timeout of .1 sec)
      thread4.test();
      assertTrue("Transaction 1 Second Thread should not have finished", thread4.isAlive()); //T1 should be waiting on T3 for B

      thread5.start();
      thread5.join(100); //waits for thread to finish (timeout of .1 sec)
      thread5.test();
      assertFalse("Transaction 1 Third Thread should have finished", thread5.isAlive()); //T1 should not be blocked on C

      thread6.start();
      thread6.join(100); //waits for thread to finish (timeout of .1 sec)
      thread6.test();
      assertFalse("Transaction 2 Second Thread should have finished", thread6.isAlive()); //T2 should not be blocked on C
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

    try {
      thread7.start();
      thread7.join(100); //waits for thread to finish (timeout of .1 sec)
      thread7.test();
      fail("Deadlock Exception not thrown.");
    } catch (DeadlockException d) {

    }

  }

  @Test
  public void testNoSelfLoopsCycleDeadlock() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    try {
      thread1.start();
      thread1.join(100); //waits for thread to finish (timeout of .1 sec)
      thread1.test();
      assertFalse("Transaction 1 Thread should have finished", thread1.isAlive()); //T1 should not be blocked

      thread2.start();
      thread2.join(100); //waits for thread to finish (timeout of .1 sec)
      thread2.test();
      assertFalse("Transaction 2 Thread should have finished", thread2.isAlive()); //T2 should not be blocked

      thread3.start();
      thread3.join(100); //waits for thread to finish (timeout of .1 sec)
      thread3.test();
      assertTrue("Transaction 2 Second Thread should not have finished", thread3.isAlive()); //T2 should be blocked
    } catch (DeadlockException d) {
      fail("No deadlock exists but Deadlock Exception was thrown.");
    }

  }


  @Test
  @Category(StudentTestP3.class)
  public void testDirectedCycleDetector() {
    WaitsForGraph g = new WaitsForGraph();
    g.addNode(1);
    g.addNode(2);
    g.addNode(3);
    g.addNode(4);

    g.addEdge(1, 2);
    g.addEdge(2, 3);
    g.addEdge(3, 4);

    assertTrue(g.edgeCausesCycle(4, 1));

    g.removeEdge(1, 2);
    g.removeEdge(2, 3);
    g.removeEdge(3, 4);

    assertTrue(g.containsNode(1));
    assertTrue(g.containsNode(2));
    assertTrue(g.containsNode(3));
    assertTrue(g.containsNode(4));
    assertFalse(g.edgeExists(4, 1));
  }

  @Test
  @Category(StudentTestP3.class)
  public void testLargeDependency() {
    WaitsForGraph g = new WaitsForGraph();
    g.addNode(1);
    g.addNode(2);
    g.addNode(3);
    g.addNode(4);

    g.addEdge(1, 4);
    g.addEdge(2, 4);
//    g.addEdge();
    assertFalse(g.edgeCausesCycle(3, 4));
    assertFalse(g.edgeCausesCycle(1, 2));
    assertTrue(g.edgeCausesCycle(4, 1));

    assertTrue(g.edgeCausesCycle(4, 4));
  }


  @Test
  @Category(StudentTestP3.class)
  public void testDeadLockIsNotPersistent() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 2, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 2 Second Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("B", 1, LockManager.LockType.EXCLUSIVE);
      }
    }, "Transaction 1 Second Thread");


  try{
    thread1.start();
    thread1.join(100);

    thread2.start();
    thread2.join(100);

    thread3.start();
    thread3.join(100);

  } catch (DeadlockException d) {
    fail("No deadlock exists but Deadlock Exception was thrown.");
  }

    try {
    thread4.start();
    thread4.join(100);
    thread4.test();
    fail("Deadlock Exception not thrown.");
  } catch (DeadlockException d) {

  }

  lockMan.releaseLock("A", 1);
  lockMan.releaseLock("B", 2);
  try {
    thread4.join(100);
    thread4.test();
    fail("Deadlock Exception not thrown.");
  } catch (DeadlockException d) {

  }

  }

  @Test(expected =  NullPointerException.class)
  @Category(StudentTestP3.class)
  public void testComplexCycleThenNoCycleThenCycleAndNPE() {
    WaitsForGraph g = new WaitsForGraph();
    for (int i = 0; i < 10; i++) {
      g.addNode(i);
    }

    for (int i= 0, j = 1; j < 10; i++, j++) {
      g.addEdge(i, j);
    }

    g.addEdge(0, 9);
    g.addEdge(9, 0);
    g.addEdge(0, 5);
    g.addEdge(5, 0);
    assertFalse(g.getCycleSwitch());
    g.removeEdge(5, 0);
    g.removeEdge(0, 9);
    assertTrue(g.edgeCausesCycle(9, 5));

    g.addEdge(15, 20);

  }

  @Test
  @Category(StudentTestP3.class)
  public void testAllSharedLocksNoDeadlocks() throws InterruptedException {
    final LockManager lockMan = new LockManager();
    AsyncDeadlockTesterThread thread1 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 1, LockManager.LockType.SHARED);
      }
    }, "Transaction 1 Thread");

    AsyncDeadlockTesterThread thread2 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 2, LockManager.LockType.SHARED);
      }
    }, "Transaction 2 Thread");

    AsyncDeadlockTesterThread thread3 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 3, LockManager.LockType.SHARED);
      }
    }, "Transaction 3  Thread");

    AsyncDeadlockTesterThread thread4 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 4, LockManager.LockType.SHARED);
      }
    }, "Transaction 4  Thread");


    AsyncDeadlockTesterThread thread5 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 5, LockManager.LockType.SHARED);
      }
    }, "Transaction 5  Thread");


    AsyncDeadlockTesterThread thread6 = new AsyncDeadlockTesterThread(new Runnable() {
      public void run() {
        lockMan.acquireLock("A", 6, LockManager.LockType.SHARED);
      }
    }, "Transaction 6  Thread");

    try {
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

      for (int i = 1; i != 7; i++) {
        assertTrue(lockMan.holdsLock("A", i, LockManager.LockType.SHARED));
      }


    } catch (DeadlockException d) {
      fail("No Deadlocks but Deadlock was thrown, silly!");
    }





  }

}

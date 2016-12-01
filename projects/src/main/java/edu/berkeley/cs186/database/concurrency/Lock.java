package edu.berkeley.cs186.database.concurrency;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Each table will have a lock object associated with it in order
 * to implement table-level locking. The lock will keep track of its
 * transaction owners, type, and the waiting queue.
 */
public class Lock {


  private Set<Long> transactionOwners;
  private ConcurrentLinkedQueue<LockRequest> transactionQueue;
  private LockManager.LockType type;

  public Lock(LockManager.LockType type) {
    this.transactionOwners = new HashSet<Long>();
    this.transactionQueue = new ConcurrentLinkedQueue<LockRequest>();
    this.type = type;
  }

  protected Set<Long> getOwners() {
    return this.transactionOwners;
  }

  public LockManager.LockType getType() {
    return this.type;
  }

  private void setType(LockManager.LockType newType) {
    this.type = newType;
  }

  public int getSize() {
    return this.transactionOwners.size();
  }

  public boolean isEmpty() {
    return this.transactionOwners.isEmpty();
  }

  private boolean containsTransaction(long transNum) {
    return this.transactionOwners.contains(transNum);
  }

  private void addToQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.add(lockRequest);
  }

  private void removeFromQueue(long transNum, LockManager.LockType lockType) {
    LockRequest lockRequest = new LockRequest(transNum, lockType);
    this.transactionQueue.remove(lockRequest);
  }

  private void addOwner(long transNum) {
    this.transactionOwners.add(transNum);
  }

  private void removeOwner(long transNum) {
    this.transactionOwners.remove(transNum);
  }

  /**
   * Attempts to resolve the specified lockRequest. Adds the request to the queue
   * and calls wait() until the request can be promoted and removed from the queue.
   * It then modifies this lock's owners/type as necessary.
   * @param transNum transNum of the lock request
   * @param lockType lockType of the lock request
   */
  protected synchronized void acquire(long transNum, LockManager.LockType lockType) {

    if (this.getOwners().contains(transNum)  && !this.getType().equals(LockManager.LockType.SHARED)) {
        return;
    }

    this.addToQueue(transNum, lockType); // Add to the queue regardless of any conditions :P

    while (!isValidForPromotion(transNum, lockType)) {
       try {
           this.wait();
       } catch (Exception e) {
           e.printStackTrace();
       }
    }

    this.removeFromQueue(transNum, lockType);
    this.addOwner(transNum);
    this.setType(lockType);

    return;
  }

    /**
     * /
     * @return whether this lock is availiable for promotion or not
     */
  private boolean isValidForPromotion(long transNum, LockManager.LockType lockType) {
      ConcurrentLinkedQueue q = transactionQueue;
      LockRequest req = new LockRequest(transNum, lockType);

      if (this.isEmpty() && q.peek().equals(req)) {
          return true;
      }
      else {
          // Case 1: UPGRADING:
          if (lockType.equals(LockManager.LockType.EXCLUSIVE)) {
              if (this.getSize() == 1 && this.containsTransaction(transNum)) {

                  return true;
              }
              return false;
          }
          if (this.getType().equals(LockManager.LockType.EXCLUSIVE)) {
              return false;
          }

         // Request is in the front of the queue
         else if (q.peek().equals(req)) {
              if (lockType.equals(LockManager.LockType.SHARED)) {
                  return true;
              }
              return false;
          }


         // For shared lock and only shared locks in the queue
         else {
              if (lockType.equals(LockManager.LockType.SHARED)) { // ths.getType == blah makes it look foreva
                  Iterator<LockRequest> lockRequests = q.iterator();
                  LockRequest c;// = lockRequests.next();
                  while (lockRequests.hasNext()) {
                      c = lockRequests.next();
                      if (c.lockType.equals(LockManager.LockType.EXCLUSIVE)) {
                          return false;
                      }

                  }
                  return true;
              }
         }
         return false;
      }
  }

  /**
   * transNum releases ownership of this lock
   * @param transNum transNum of transaction that is releasing ownership of this lock
   */
  protected synchronized void release(long transNum) {
      // TODO: Implement Me!!

      this.removeOwner(transNum);
//      this.tr
      if (this.getOwners().isEmpty()) {
          this.setType(LockManager.LockType.SHARED);
      }
      this.notifyAll();

      return;

  }

  /**
   * Checks if the specified transNum holds a lock of lockType on this lock object
   * @param transNum transNum of lock request
   * @param lockType lock type of lock request
   * @return true if transNum holds the lock of type lockType
   */
  protected synchronized boolean holds(long transNum, LockManager.LockType lockType) {
      //TODO: Implement Me!!f
      if (this.getOwners().contains(transNum) && this.getType() == lockType) {
          return true;
      } else {
          return false;
      }
  }

  /**
   * LockRequest objects keeps track of the transNum and lockType.
   * Two LockRequests are equal if they have the same transNum and lockType.
   */
  private class LockRequest {
      private long transNum;
      private LockManager.LockType lockType;
      private LockRequest(long transNum, LockManager.LockType lockType) {
        this.transNum = transNum;
        this.lockType = lockType;
      }

      @Override
      public int hashCode() {
        return (int) transNum;
      }

      @Override
      public boolean equals(Object obj) {
        if (!(obj instanceof LockRequest))
          return false;
        if (obj == this)
          return true;

        LockRequest rhs = (LockRequest) obj;
        return (this.transNum == rhs.transNum) && (this.lockType == rhs.lockType);
      }

  }

}

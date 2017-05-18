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
    //TODO: Implement Me!!
    LockRequest req = new LockRequest(transNum, lockType);
//    if (compatible(req)) {
//        addOwner(req.transNum);
//        setType(req.lockType);
//    } else {
        //if this transaction is already an owner of an exclusive lock...
        if (containsTransaction(transNum) && getType().equals(LockManager.LockType.EXCLUSIVE)) {
            return;
        }
        addToQueue(req.transNum, req.lockType);
        while (!upgrade(req) && (!compatible(req) || !canPromote(req))) { //replace with while?
            try {
                wait();
            } catch (InterruptedException e) {}
        }
//        System.out.println("granting...");
        removeFromQueue(req.transNum, req.lockType);
        addOwner(req.transNum);
        setType(req.lockType);
//        if (req.lockType.equals(LockManager.LockType.EXCLUSIVE)) {
//            //remove all others from owners
//            for (Long owner : getOwners()) {
//                removeOwner(owner);
//            }
//            //now make this the owner
//            addOwner(req.transNum);
//            setType(req.lockType);
//        } else {
//            addOwner(req.transNum);
//            setType(req.lockType);
//        }
//    }
  }

    /**
     * this checks to see if this LockRequest is ready for promotion
     * if exactly (any?) one of these three conditions holds:
     * 1) the request is an upgrade from shared to exclusive
     * 2) the request is in front of the queue
     * 3) the request is for a shared lock and there are only other shared locks in front of it
     *    in the queue
     * @param req LockRequest
     * @return true if it can be promoted
     */
  private boolean canPromote(LockRequest req) {
      int count = 0;
      boolean first = transactionQueue.peek().equals(req);
      boolean upgrade = upgrade(req);
      boolean onlyShared = !req.lockType.equals(LockManager.LockType.EXCLUSIVE);

      Iterator<LockRequest> iter = transactionQueue.iterator();

      while (onlyShared && iter.hasNext()) {
          LockRequest curr = iter.next();
          if (curr.equals(req)) {
              break;
          } else if (curr.lockType.equals(LockManager.LockType.EXCLUSIVE)){
              onlyShared = false;
          }
      }

      if (first) {count++;}
      if (upgrade) {count++;}
      if (onlyShared) {count++;}

      return count >= 1;
  }

    /**
     * Checks to see if the requested lock is compatible with the current locks owned
     * @param req a LockRequest
     * @return true if this request is compatible with the current lock
     */
  private boolean compatible(LockRequest req) {
      if (getSize() == 0) {
          return true;
      } else if (req.lockType.equals(LockManager.LockType.EXCLUSIVE)) {
          return false;
      }
      return req.lockType.equals(getType());
  }

  private boolean upgrade(LockRequest req) {
      return containsTransaction(req.transNum) && req.lockType.equals(LockManager.LockType.EXCLUSIVE) && getSize() == 1;
  }

  /**
   * transNum releases ownership of this lock
   * @param transNum transNum of transaction that is releasing ownership of this lock
   */
  protected synchronized void release(long transNum) {
    //TODO: Implement Me!!
    removeOwner(transNum);
//    if (getType().equals(LockManager.LockType.EXCLUSIVE)) {
//        //go down the queue and upon the first exclusive lock found
//        for (LockRequest req : transactionQueue) {
//            if (upgrade(req)) {
//                addOwner(req.transNum);
//                removeFromQueue(req.transNum, req.lockType);
//                break;
//            }
//        }
//    }
    notifyAll();
  }

  /**
   * Checks if the specified transNum holds a lock of lockType on this lock object
   * @param transNum transNum of lock request
   * @param lockType lock type of lock request
   * @return true if transNum holds the lock of type lockType
   */
  protected synchronized boolean holds(long transNum, LockManager.LockType lockType) {
    //TODO: Implement Me!!
      return containsTransaction(transNum) && lockType.equals(getType());
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

package cbpdbms;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import util.Graph;

/**
 * Inner class for lock management
 */
public class LockManager {
	Map<TransactionId, Set<PageId>> sharedpage;// tid sharedLock page
	Map<TransactionId, Set<PageId>> exclusivepage;// tid exclusivelock page
	// Map<TransactionId, Long> timeMap;// tid
	Lock lock;// acquirelock releaselock

	Graph<TransactionId> wfGraph;// wait-for graph

	public LockManager() {
		sharedpage = new ConcurrentHashMap<TransactionId, Set<PageId>>();
		exclusivepage = new ConcurrentHashMap<TransactionId, Set<PageId>>();
		// timeMap = new ConcurrentHashMap<TransactionId, Long>();
		lock = new ReentrantLock();
		wfGraph = new Graph<TransactionId>("wait-for");
	}

	public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
		if (sharedpage.containsKey(tid)) {
			Set<PageId> set = sharedpage.get(tid);
			if (set.contains(pid))
				return true;
		}

		if (exclusivepage.containsKey(tid)) {
			Set<PageId> set = exclusivepage.get(tid);
			if (set.contains(pid))
				return true;
		}

		return false;
	}

	/**
	 * 
	 * @param tid
	 */
	public void releaseAllTransactionLock(TransactionId tid) {
		// exclusivelock
		if (exclusivepage.get(tid) != null)
			for (PageId pid : exclusivepage.get(tid)) {
				HeapPageId hpid = (HeapPageId) pid;
				hpid.exclusiveLock.unlock(tid, hpid);
			}
		// tid exclusivepage
		exclusivepage.remove(tid);

		// sharedlock
		if (sharedpage.get(tid) != null)
			for (PageId pid : sharedpage.get(tid)) {
				HeapPageId hpid = (HeapPageId) pid;
				hpid.sharedLock.unlock(tid, hpid);
			}
		// tid sharedpage 
		sharedpage.remove(tid);

		// hack : only for HeapFileReadTest
		// HeapFileReadTest getPage，wfGraph tid。
		if (wfGraph.findNode(tid) == null)
			return;

		// wait-for graph
		wfGraph.delNode(tid);
	}

	/**
	 * 
	 * @param tid
	 * @param pid
	 */
	public void releaseLock(TransactionId tid, PageId pid) {
		lock.lock();
		HeapPageId hpid = (HeapPageId) pid;
		hpid.sharedLock.unlock(tid, pid);
		hpid.exclusiveLock.unlock(tid, pid);

		// wait-for graph
		// tid node，wait-for graph
		if (this.exclusivepage.size() == 0 && this.sharedpage.size() == 0)
			wfGraph.delNode(tid);
		lock.unlock();
	}

	/**
	 * 
	 * @param tid
	 * @param pid
	 * @param perm
	 * @return true is deadlock, false if nodead
	 */
	private boolean deadlockCheck(TransactionId tid, PageId pid, Permissions perm) {
		HeapPageId hpid = (HeapPageId) pid;
		// step1 tid node wait-for graph
		wfGraph.addNode(tid);
		// step2 lock
		boolean granted = true;
		if (perm.equals(Permissions.READ_ONLY))
			granted = hpid.sharedLock.canlock(tid, hpid);
		else
			granted = hpid.exclusiveLock.canlock(tid, hpid);

		// step3 granted wait
		if (granted) {
			return false;
		} else {
			HashSet<TransactionId> lockedSet = new HashSet<TransactionId>();

			// nullset
			if (hpid.lock.exclusivePage != null)
				lockedSet.add(hpid.lock.exclusivePage);

			if (perm.equals(Permissions.READ_WRITE))
				lockedSet.addAll(hpid.lock.sharedPage);

			lockedSet.remove(tid);

			// wait
			for (TransactionId t : lockedSet) {
				wfGraph.addEdge(tid, t);
			}
		}

		// step4 
		
		boolean isCycle = wfGraph.isCycle(tid);
		if (isCycle) {
			// wfGraph.delNode(tid);
			// Abort
			return true;
		} else {
			
			return false;

		}
	}

	/**
	 * PageSharedExclusiveLock true false，LockManager lock
	 * 
	 * @param tid
	 *            the transaction that want to acquire the lock
	 * @param pid
	 *            which page the tid want lock-in
	 * @param perm
	 *            shared or exclusive
	 */
	public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		// lock.lock();
		
		HeapPageId hpid = (HeapPageId) pid;

		try {
			// wait-for
			synchronized (this) {  // sync getPage()
				boolean deadlock = deadlockCheck(tid, pid, perm);
				if (deadlock)
					throw new TransactionAbortedException();
			}

			if (perm.equals(Permissions.READ_ONLY)) {
				boolean granted = false;

				while (!granted) {// lock
					granted = hpid.sharedLock.lock(tid, hpid);
				}

				Set<PageId> set = sharedpage.get(tid);
				if (set == null)
					set = new HashSet<PageId>();
				set.add(pid);
				sharedpage.put(tid, set);
			} else {
				boolean granted = false;
				while (!granted) {
					granted = hpid.exclusiveLock.lock(tid, hpid);
				}

				Set<PageId> set = exclusivepage.get(tid);
				if (set == null)
					set = new HashSet<PageId>();
				set.add(pid);
				exclusivepage.put(tid, set);

				// wfGraph.delNode(tid);
				// NOTE: wait-for graph node
				// see releaseLock()
			}
		} finally {
		}
	}
}

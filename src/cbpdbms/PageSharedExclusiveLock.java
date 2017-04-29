package cbpdbms;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import util.Debug;

public class PageSharedExclusiveLock {
	public Set<TransactionId> sharedPage;// tid
	public TransactionId exclusivePage;// tid
	Lock lock;
	public SharedLock sharedLock;
	public ExclusiveLock exclusiveLock;

	public PageSharedExclusiveLock() {
		// writer = false;
		// readers = 0;
		sharedPage = new HashSet<TransactionId>();
		exclusivePage = null;

		lock = new ReentrantLock();
		sharedLock = new SharedLock();
		exclusiveLock = new ExclusiveLock();
		// condition = lock.newCondition();
	}

	public SharedLock readLock() {
		return sharedLock;
	}

	public ExclusiveLock writeLock() {
		return exclusiveLock;
	}

	public class SharedLock {
		/**
		 * 
		 * @param tid
		 * @param pid
		 * @return true if can get lock, false if not.
		 */
		public boolean canlock(TransactionId tid, PageId pid) {
			boolean granted = false;

			try {// exclusiveLock

				if (!(exclusivePage == null || exclusivePage == tid)) {
					granted = false;
				} else if (exclusivePage == null) {
					granted = true;
				} else {
					granted = true;
				}
			} finally {
			}

			return granted;

		}

		/**
		 * 
		 * @param tid
		 * @param pid
		 * @return true if get the lock, false if not.
		 */
		public boolean lock(TransactionId tid, PageId pid) {
			lock.lock();
			Debug.printInfo("ThreadId:" + Thread.currentThread().getId() + "get lock");

			boolean granted = false;

			try {//exclusiveLock

				Debug.printInfo(tid + "want to get sharedlock on " + pid);
				if (!(exclusivePage == null || exclusivePage == tid)) {
					Debug.printInfo("tid");

					// LockingTest
					// condition.await(200, TimeUnit.MILLISECONDS);
					granted = false;
				} else if (exclusivePage == null) {
					Debug.printInfo(tid + " ");
					sharedPage.add(tid);
					// readers++;
					granted = true;
				} else {
					Debug.printInfo(tid + ",");
					granted = true;
				}
			} finally {
				Debug.printInfo("ThreadId:" + Thread.currentThread().getId() + "unlock");
				lock.unlock();
			}

			return granted;

		}

		public void unlock(TransactionId tid, PageId pid) {
			lock.lock();
			Debug.printInfo("ThreadId:" + Thread.currentThread().getId() + "get lock");
			try {
				if (sharedPage.remove(tid)) {
					// readers--;
					Debug.printInfo(tid + "" + pid + "");
				}
				// if (readers == 0)
				// condition.signalAll();

			} finally {
				Debug.printInfo("ThreadId:" + Thread.currentThread().getId() + "unlock");
				lock.unlock();
			}
		}

	}

	public class ExclusiveLock {
		public boolean canlock(TransactionId tid, PageId pid) {
			boolean granted = false;
			// case1 0， null tid
			// case2 1， 
			//
			try {
				// if (!(((readers == 0) && (exclusivePage == null ||
				// exclusivePage == tid)) || ((readers == 1) && (sharedPage
				// .contains(tid)))))
				// reader writers
				if (!(((sharedPage.size() == 0) && (exclusivePage == null || exclusivePage == tid))
						|| ((sharedPage.size() == 1) && (sharedPage.contains(tid)))))

				{
					granted = false;
					// exclusivePage,
					// tid exclusivelock
				} else if (sharedPage.size() == 1) {
					granted = true;
				} else if (exclusivePage == tid) {
					granted = true;
				} else {
					granted = true;
				}

			} finally {
			}

			return granted;
		}

		public boolean lock(TransactionId tid, PageId pid) {

			lock.lock();
			Debug.printInfo("ThreadId:" + Thread.currentThread().getId());
			boolean granted = false;

			try {
				Debug.printInfo(tid + "want to get exclusivelock on " + pid);
				if (!(((sharedPage.size() == 0) && (exclusivePage == null || exclusivePage == tid))
						|| ((sharedPage.size() == 1) && (sharedPage.contains(tid))))) {

					if (exclusivePage != null && exclusivePage != tid)
						Debug.printInfo(pid + "" + exclusivePage + "");
					else if (sharedPage.size() > 1 || (sharedPage.size() == 1 && !sharedPage.contains(tid)))
						Debug.printInfo("tid:" + sharedPage);
					else
						Debug.printInfo("");

					granted = false;
				}

				else if (sharedPage.size() == 1) {
					Debug.printInfo("upgrading lock!!!");
					// readers--;
					sharedPage.remove(tid);
					granted = true;
					exclusivePage = tid;
					// writer = true;
				} else if (exclusivePage == tid) {
					Debug.printInfo(tid + "" + pid + "");
					granted = true;
					exclusivePage = tid;
					// writer = true;
				} else {
					Debug.printInfo("" + pid + "");
					granted = true;
					exclusivePage = tid;
					// writer = true;
				}

			} finally {
				Debug.printInfo("ThreadId:" + Thread.currentThread().getId() + "unlock");
				lock.unlock();
			}

			return granted;

		}

		public void unlock(TransactionId tid, PageId pid) {
			lock.lock();

			Debug.printInfo("ThreadId:" + Thread.currentThread().getId() + "get lock");
			if (exclusivePage == tid) {
				// writer = false;
				exclusivePage = null;
				if (control.CommandLine.TEST_LOCK)
					System.out.println(tid + "" + pid + "");
				// condition.signalAll();
			}
			Debug.printInfo("ThreadId:" + Thread.currentThread().getId() + "unlock");
			lock.unlock();
		}

	}

}

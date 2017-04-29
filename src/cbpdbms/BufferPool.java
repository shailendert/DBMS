package cbpdbms;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import util.Debug;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool which check that the transaction has the appropriate locks
 * to read/write the page.
 */
public class BufferPool {
	/** Bytes per page, including header. */
	public static final int PAGE_SIZE = 4096;

	/**
	 * Default number of pages passed to the constructor. This is used by other
	 * classes. BufferPool should use the numPages argument to the constructor
	 * instead.
	 */
	public static final int DEFAULT_PAGES = 50;

	int numPages;
	public Map<PageId, Page> pageMap;
	List<Page> clockList; // recent used
	LockManager locker;
	Map<PageId, PageId> pageIdMap;

	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages
	 *            maximum number of pages in this buffer pool.
	 */

	public BufferPool(int numPages) {
		this.numPages = numPages;
		this.pageMap = new ConcurrentHashMap<PageId, Page>();
		this.clockList = new LinkedList<Page>();
		this.locker = new LockManager();

		this.pageIdMap = new ConcurrentHashMap<PageId, PageId>();
	}

	/**
	 * 
	 */
	public int getPageCount() {
		return this.pageMap.keySet().size();
	}

	/**
	 * Retrieve the specified page with the associated permissions. Will acquire
	 * a lock and may block if that lock is held by another transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool. If it is
	 * present, it should be returned. If it is not present, it should be added
	 * to the buffer pool and returned. If there is insufficient space in the
	 * buffer pool, an page should be evicted and the new page should be added
	 * in its place.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the page
	 * @param pid
	 *            the ID of the requested page
	 * @param perm
	 *            the requested permissions on the page
	 */
	public Page getPage(TransactionId tid, PageId _pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		PageId pid;

		// NOTE: gap.
		// t1.get，3, t2.get null，pid null，2

		synchronized (this) {
			pid = pageIdMap.get(_pid);// 1
			if (pid == null)
				pageIdMap.put(_pid, _pid);// 2
			pid = pageIdMap.get(_pid);// 3
		}

		acquireLock(tid, pid, perm);// 
		// some code goes here
		if (pageMap.containsKey(pid)) {
			HeapPage page = (HeapPage) pageMap.get(pid);

			// RUList.remove(page);
			// RUList.add(page);
			page.setVisit(true);

			return page;
		} else {
			int fileId = pid.getTableId();

			List<Catalog.TableItem> tlist = Database.getCatalog().getTableItem();
			for (Catalog.TableItem t : tlist) {
				// catalog file，file page。
				int tableid = t.getFile().getId();
				if (tableid != fileId)
					continue;

				// verify BufferPool is full
				if (pageMap.keySet().size() == this.numPages) {
					// evict a page
					evictPage();
				}
				DbFile file = Database.getCatalog().getDbFile(fileId);
				// disk！！！！！
				Page newpage = file.readPage(pid);
				// page bufferpool
				pageMap.put(pid, newpage);
				clockList.add(newpage);

				return newpage;

			}

			throw new DbException("file not in catalog");

		}
	}

	public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		locker.acquireLock(tid, pid, perm);
	}

	/**
	 * Releases the lock on a page. Calling this is very risky, and may result
	 * in wrong behavior. Think hard about who needs to call this and why, and
	 * why they can run the risk of calling it.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param pid
	 *            the ID of the page to unlock
	 */
	public void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		this.locker.releaseLock(tid, pid);

	}

	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}

	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		return locker.holdsLock(tid, pid);
	}

	/**
	 * Commit or abort a given transaction; release all locks associated to the
	 * transaction.
	 *
	 * @param tid
	 *            the ID of the transaction requesting the unlock
	 * @param commit
	 *            a flag indicating whether we should commit or abort. commit if
	 *            true, abort if false.
	 */
	public synchronized void transactionComplete(TransactionId tid, boolean commit) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		if (commit == true) {// commit
			Debug.printInfo("-----------Commit transaction-----------");
			for (Page page : pageMap.values()) {
				if (page.isDirty() != null && page.isDirty().equals(tid)) {
					// NOTE: add in lab5-------------------------
					// use current page contents as the before-image
					// for the next transaction that modifies this page.
					page.setBeforeImage();
					// ------------------------------------------
					flushPage(page.getId());
				}
			}
		} else {// abort
			Debug.printInfo("-------------Abort transaction----------");
			for (Page page : pageMap.values()) {
				if (page.isDirty() != null && page.isDirty().equals(tid))
					pageMap.put(page.getId(), page.getBeforeImage());
			}
		}

		// commit，tid
		// locker.timeMap.remove(tid);
		locker.releaseAllTransactionLock(tid);
		Debug.printInfo("----------------------------------------");
	}

	/**
	 * Add a tuple to the specified table behalf of transaction tid. Will
	 * acquire a write lock on the page the tuple is added to(Lock acquisition
	 * is not needed for lab2). May block if the lock cannot be acquired.
	 * 
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and updates cached versions of any pages that have
	 * been dirtied so that future requests see up-to-date pages.
	 *
	 * @param tid
	 *            the transaction adding the tuple
	 * @param tableId
	 *            the table to add the tuple to
	 * @param t
	 *            the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		DbFile dbfile = Database.getCatalog().getDbFile(tableId);

		// TODO: need to deal with return modify page
		// move log operation and write to here??
		dbfile.addTuple(tid, t);
	}

	/**
	 * Remove the specified tuple from the buffer pool. Will acquire a write
	 * lock on the page the tuple is removed from. May block if the lock cannot
	 * be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit. Does not need to update cached versions of any pages
	 * that have been dirtied, as it is not possible that a new page was created
	 * during the deletion (note difference from addTuple).
	 *
	 * @param tid
	 *            the transaction adding the tuple.
	 * @param t
	 *            the tuple to add
	 */
	public void deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		RecordId rid = t.getRecordId();
		PageId pid = rid.getPageId();

		if (!this.pageMap.containsKey(pid))
			throw new DbException("the Tuple is not in bufferpool");

		Page page = this.getPage(tid, pid, Permissions.READ_WRITE);
		HeapPage hpage = (HeapPage) page;
		hpage.deleteTuple(t);
		hpage.markDirty(true, tid);

	}

	/**
	 * Flush all dirty pages to disk. NB: Be careful using this routine -- it
	 * writes dirty data to disk so will break simpledb if running in NO STEAL
	 * mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for lab1
		for (Page page : pageMap.values()) {
			if (page.isDirty() != null) {
				// NOTE: add in lab5-------------------------
				// use current page contents as the before-image
				// for the next transaction that modifies this page.
				page.setBeforeImage();
				// ------------------------------------------
				flushPage(page.getId());
			}
		}

	}

	/**
	 * Remove the specific page id from the buffer pool. Needed by the recovery
	 * manager to ensure that the buffer pool doesn't keep a rolled back page in
	 * its cache.
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// only necessary for lab5
	}

	/**
	 * Flushes a certain page to disk.
	 * 
	 * Log record must write to disk before the database elements write to the
	 * disk
	 * 
	 * @param pid
	 *            an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for lab1
		int tableid = pid.getTableId();
		DbFile file = Database.getCatalog().getDbFile(tableid);
		Page page = pageMap.get(pid);

		// page.markDirty(false, null);

		// NOTE add in lab5 markDirty must behind the logWrite
		// append an update record to the log, with
		// a before-image and after-image.
		TransactionId dirtier = page.isDirty();
		if (dirtier != null) {
			// write-ahead logging
			Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
			Database.getLogFile().force();
		}
		// -----------------------------------
		page.markDirty(false, null);
		// Write to disk
		file.writePage(page);
		Debug.printInfo("flushPage:" + pid);

	}

	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2|lab3
		for (Page page : pageMap.values()) {
			if (page.isDirty() != null && page.isDirty().equals(tid)) {
				// NOTE: add in lab5-------------------------
				// use current page contents as the before-image
				// for the next transaction that modifies this page.
				HeapPage hpage = (HeapPage) page;
				byte before[] = hpage.oldData;
				page.setBeforeImage();

				byte data[] = hpage.oldData;
				// ------------------------------------------
				flushPage(page.getId());
			}
		}
	}

	/**
	 * Discards a page from the buffer pool. Flushes the page to disk to ensure
	 * dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {// NO STEAL

		// cleanPage，pageMap remove。

		// step 1
		for (Page pp : clockList) {
			HeapPage p = (HeapPage) pp;
			if (p.isDirty() == null && p.getVisit() == false) {
				clockList.remove(p);
				pageMap.remove(p.getId());
				pageIdMap.remove(p.getId());
				return;
			}
			p.setVisit(false);
		}

		// step 2
		for (Page pp : clockList) {
			HeapPage p = (HeapPage) pp;
			if (p.isDirty() == null && p.getVisit() == false) {
				clockList.remove(p);
				pageMap.remove(p.getId());
				pageIdMap.remove(p.getId());
				return;
			}
		}
		throw new DbException("no clean page for evict");
	}

}

package cbpdbms;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see cbpdbms.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	File f;
	TupleDesc td;

	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		this.f = f;
		this.td = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return this.f;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		// return num;
		return this.f.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return this.td;
	}


	public Page readPage(PageId pid) {
		try {
			
			RandomAccessFile rAf = new RandomAccessFile(f, "r");
			// page
			int offset = pid.pageno() * BufferPool.PAGE_SIZE;
			
			rAf.seek(offset);
			// page
			byte[] page = new byte[BufferPool.PAGE_SIZE];
			// BufferPool.PAGE_SIZE
			rAf.read(page, 0, BufferPool.PAGE_SIZE);
			rAf.close();

			HeapPageId id = (HeapPageId) pid;

			return new HeapPage(id, page);
		} catch (IOException e) {
			e.printStackTrace();
		}
		throw new IllegalArgumentException();
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for lab1
		PageId pid = page.getId();
		// int tableid = pid.getTableId();
		int pgno = pid.pageno();
		byte[] data = page.getPageData();

		RandomAccessFile rAf = new RandomAccessFile(this.f, "rw");
		rAf.seek(pgno * BufferPool.PAGE_SIZE);
		rAf.write(data, 0, BufferPool.PAGE_SIZE);
		rAf.close();

	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		long file_size = this.f.length();
		int num = (int) (file_size / BufferPool.PAGE_SIZE);
		if (file_size % BufferPool.PAGE_SIZE > 0)
			num++;
		return num;
	}

	/**
	 * 
	 * @return
	 */
	public ArrayList<Page> getEmptyPageList(TransactionId tid) {
		ArrayList<Page> emptylist = new ArrayList<Page>();

		for (int i = 0; i < this.numPages(); i++) {
			PageId pid = new HeapPageId(this.getId(), i);
			try {
				// NOTE: intertTuple need exclusive lock
				Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
				HeapPage hpage = (HeapPage) page;
				if (hpage.getNumEmptySlots() > 0)
					emptylist.add(page);
			} catch (TransactionAbortedException | DbException e) {
				e.printStackTrace();
			}

		}
		return emptylist;
	}

	/**
	 * Tuple.
	 */
	public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		ArrayList<Page> emptyPage = getEmptyPageList(tid);
		ArrayList<Page> modifiedPage = new ArrayList<Page>();
		Page page;
		if (emptyPage.isEmpty()) {
			// create new emptypage in current file
			
			HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
			page = new HeapPage(pid, HeapPage.createEmptyPageData());
			HeapPage hpage = (HeapPage) page;
			hpage.addTuple(t);

			// NOTE:and append it to the physical disk?? write-ahead log??

			// Database.getBufferPool().flushPage(hpage.getId());
			hpage.markDirty(true, tid);// ???

			TransactionId dirtier = page.isDirty();
			if (dirtier != null) {
				// write-ahead logging
				Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
				Database.getLogFile().force();
			}
			// -----------------------------------

			/*
			 * NOTE: this.writePage should not in here. But some Test not in
			 * consideration of transaction.
			 * 
			 * In this case, writePage is append a new page to the file.
			 */
			this.writePage(page);

			modifiedPage.add(page);

		} else {
			// get a page
			page = emptyPage.get(0);
			HeapPage hpage = (HeapPage) page;
			hpage.addTuple(t);
			hpage.markDirty(true, tid);

			TransactionId dirtier = page.isDirty();
			if (dirtier != null) {
				// write-ahead logging
				Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
				Database.getLogFile().force();
			}
			// -----------------------------------
			modifiedPage.add(hpage);
		}
		return modifiedPage;

	}

	/**
	 * delete tuple in this file
	 * 
	 * @param tid
	 *            the transaction id
	 * @param t
	 *            the tuple that want to delete
	 * @return the page that modified
	 * @throws IOException
	 */
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException, IOException {
		// some code goes here
		RecordId rid = t.getRecordId();
		PageId pid = rid.getPageId();

		if (pid.getTableId() != this.getId())
			throw new DbException("the tuple not in this table");
		if (this.numPages() <= pid.pageno())
			throw new DbException("the tuple's pgno is wrong");

		Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

		HeapPage hpage = (HeapPage) page;
		hpage.deleteTuple(t);
		hpage.markDirty(true, tid);

		// add in branch 5
		TransactionId dirtier = page.isDirty();
		if (dirtier != null) {
			// write-ahead logging
			Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
			Database.getLogFile().force();
		}
		// -----------------------------------

		this.writePage(page);

		return page;
		// not necessary for lab1
	}

	/**
	 * @return a HeapFileIterator that contains all tuple in the file
	 */
	public DbFileIterator iterator(TransactionId tid) throws DbException, TransactionAbortedException {
		// page Tuple list。
		List<Tuple> ftupleList = new ArrayList<Tuple>();
		int page_count = this.numPages();

		for (int i = 0; i < page_count; i++) {
			// HeapPageId hashcode，PageId
			// Map get
			PageId pid = new HeapPageId(getId(), i);

			// NOTE:Scan only need to acquire sharedLock.
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
			Iterator<Tuple> it = page.iterator();
			while (it.hasNext())
				ftupleList.add(it.next());
		}

		return new HeapFileIterator(tid, ftupleList);
	}

}

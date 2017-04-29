package cbpdbms;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

import util.Debug;

/**
 LogFile implements the recovery subsystem of CBPDBMS.  This class is
 able to write different log records as needed, but it is the
 responsibility of the caller to ensure that write ahead logging and
 two-phase locking discipline are followed.  <p>

 <u> Locking note: </u>
 <p>

 Many of the methods here are synchronized (to prevent concurrent log
 writes from happening); many of the methods in BufferPool are also
 synchronized (for similar reasons.)  Problem is that BufferPool writes
 log records (on page flushed) and the log file flushes BufferPool
 pages (on checkpoints and recovery.)  This can lead to deadlock.  For
 that reason, any LogFile operation that needs to access the BufferPool
 must not be declared synchronized and must begin with a block like:

 <p>
 <pre>
 synchronized (Database.getBufferPool()) {
 synchronized (this) {

 ..

 }
 }
 </pre>
 */

/**
 * <p>
 * The format of the log file is as follows:
 * 
 * <ul>
 * 
 * <li>The first long integer of the file represents the offset of the last
 * written checkpoint, or -1 if there are no checkpoints
 * 
 * <li>All additional data in the log consists of log records. Log records are
 * variable length.
 * 
 * <li>Each log record begins with an integer type and a long integer
 * transaction id.
 * 
 * <li>Each log record ends with a long integer file offset representing the
 * position in the log file where the record began.
 * 
 * <li>There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and CHECKPOINT
 * 
 * <li>ABORT, COMMIT, and BEGIN records contain no additional data
 * 
 * <li>UPDATE RECORDS consist of two entries, a before image and an after image.
 * These images are serialized Page objects, and can be accessed with the
 * LogFile.readPageData() and LogFile.writePageData() methods. See
 * LogFile.print() for an example.
 * 
 * <li>CHECKPOINT records consist of active transactions at the time the
 * checkpoint was taken and their first log record on disk. The format of the
 * record is an integer count of the number of transactions, as well as a long
 * integer transaction id and a long integer first record offset for each active
 * transaction.
 * 
 * </ul>
 */

public class LogFile {
	File logFile;
	RandomAccessFile raf;
	Boolean recoveryUndecided; // no call to recover() and no append to log

	static final int ABORT_RECORD = 1;
	static final int COMMIT_RECORD = 2;
	static final int UPDATE_RECORD = 3;
	static final int BEGIN_RECORD = 4;
	static final int CHECKPOINT_RECORD = 5;
	static final long NO_CHECKPOINT_ID = -1;

	static int INT_SIZE = 4;
	static int LONG_SIZE = 8;

	long currentOffset = -1;
	// int pageSize;
	int totalRecords = 0; // for PatchTest

	/**
	 * key = tid, value = the posion the record start with
	 * 
	 * When start a transaction, the tid put in to the list.
	 * 
	 * When commit/abort a transaction, the tid remove from the list
	 */
	HashMap<Long, Long> tidToFirstLogRecord = new HashMap<Long, Long>();

	/**
	 * Constructor. Initialize and back the log file with the specified file.
	 * We're not sure yet whether the caller is creating a brand new DB, in
	 * which case we should ignore the log file, or whether the caller will
	 * eventually want to recover (after populating the Catalog). So we make
	 * this decision lazily: if someone calls recover(), then do it, while if
	 * someone starts adding log file entries, then first throw out the initial
	 * log file contents.
	 * 
	 * @param f
	 *            The log file's name
	 */
	public LogFile(File f) throws IOException {
		this.logFile = f;
		raf = new RandomAccessFile(f, "rw");
		recoveryUndecided = true;

		// install shutdown hook to force cleanup on close
		// Runtime.getRuntime().addShutdownHook(new Thread() {
		// public void run() { shutdown(); }
		// });

		//  WARNING -- there is nothing that verifies that the specified
		// log file actually corresponds to the current catalog.
		// This could cause problems since we log tableids, which may or
		// may not match tableids in the current catalog.
	}

	// we're about to append a log record. if we weren't sure whether the
	// DB wants to do recovery, we're sure now -- it didn't. So truncate
	// the log.
	void preAppend() throws IOException {
		totalRecords++;
		if (recoveryUndecided) {
			recoveryUndecided = false;
			raf.seek(0);
			raf.setLength(0);
			raf.writeLong(NO_CHECKPOINT_ID);
			raf.seek(raf.length());
			currentOffset = raf.getFilePointer();
		}
	}

	public int getTotalRecords() {
		return totalRecords;
	}

	/**
	 * Write an abort record to the log for the specified tid, force the log to
	 * disk, and perform a rollback
	 * 
	 * @param tid
	 *            The aborting transaction.
	 */
	public void logAbort(TransactionId tid) throws IOException {
		// must have buffer pool lock before proceeding, since this
		// calls rollback

		synchronized (Database.getBufferPool()) {
			synchronized (this) {
				preAppend();
				Debug.printLogInfo("<ABORT " + tid.getId() + ">");
				// should we verify that this is a live transaction?

				// must do this here, since rollback only works for
				// live transactions (needs tidToFirstLogRecord)
				rollback(tid);

				raf.writeInt(ABORT_RECORD);
				raf.writeLong(tid.getId());
				raf.writeLong(currentOffset);
				currentOffset = raf.getFilePointer();
				force();
				tidToFirstLogRecord.remove(tid.getId());
			}
		}
	}

	/**
	 * Write a commit record to disk for the specified tid, and force the log to
	 * disk.
	 * 
	 * @param tid
	 *            The committing transaction.
	 */
	public synchronized void logCommit(TransactionId tid) throws IOException {
		preAppend();
		Debug.printLogInfo("<COMMIT " + tid.getId() + ">");
		// should we verify that this is a live transaction?

		raf.writeInt(COMMIT_RECORD);
		raf.writeLong(tid.getId());
		raf.writeLong(currentOffset);
		currentOffset = raf.getFilePointer();
		force();
		tidToFirstLogRecord.remove(tid.getId());
	}

	/**
	 * Write an UPDATE record to disk for the specified tid and page (with
	 * provided before and after images.)
	 * 
	 * @param tid
	 *            The transaction performing the write
	 * @param before
	 *            The before image of the page
	 * @param after
	 *            The after image of the page
	 * @see cbpdbms.Page#getBeforeImage
	 */
	public synchronized void logWrite(TransactionId tid, Page before, Page after) throws IOException {
		Debug.printLogInfo("WRITE, offset = " + raf.getFilePointer());
		preAppend();
		/*
		 * update record conists of
		 * 
		 * record type transaction id before page data (see writePageData) after
		 * page data start offset
		 */
		raf.writeInt(UPDATE_RECORD);
		raf.writeLong(tid.getId());

		writePageData(raf, before);
		writePageData(raf, after);
		raf.writeLong(currentOffset);
		currentOffset = raf.getFilePointer();

		// Debug.printLogInfo("WRITE OFFSET = " + currentOffset);
		Debug.printLogInfo("<" + tid.getId() + ", before:" + before.getId() + ", after:" + after.getId() + ", "
				+ currentOffset + ">");
	}

	/**
	 * Write page data to log file
	 * 
	 * pageClassName|pidName|pid data|pagedata length| data|
	 * 
	 * @param raf
	 * @param p
	 * @throws IOException
	 */
	void writePageData(RandomAccessFile raf, Page p) throws IOException {
		PageId pid = p.getId();
		int pageInfo[] = pid.serialize();

		// page data is:
		// page class name
		// id class name
		// id class bytes
		// id class data
		// page class bytes
		// page class data

		String pageClassName = p.getClass().getName();

		String idClassName = pid.getClass().getName();

		// pageClassname
		raf.writeUTF(pageClassName);
		// pageidname
		raf.writeUTF(idClassName);

		// pageinfo length
		raf.writeInt(pageInfo.length);
		for (int i = 0; i < pageInfo.length; i++) {
			raf.writeInt(pageInfo[i]);
		}
		byte[] pageData = p.getPageData();
		// data length
		raf.writeInt(pageData.length);
		// data
		raf.write(pageData);
		// Debug.printLogInfo ("WROTE PAGE DATA, CLASS = " + pageClassName + ",
		// table = " + pid.getTableId() + ", page = " + pid.pageno());
	}

	/**
	 * 
	 * @param raf
	 * @return
	 * @throws IOException
	 */
	Page readPageData(RandomAccessFile raf) throws IOException {
		PageId pid;
		Page newPage = null;

		String pageClassName = raf.readUTF();
		String idClassName = raf.readUTF();

		try {
			Class<?> idClass = Class.forName(idClassName);
			Class<?> pageClass = Class.forName(pageClassName);

			Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
			int numIdArgs = raf.readInt();
			Object idArgs[] = new Object[numIdArgs];
			for (int i = 0; i < numIdArgs; i++) {
				idArgs[i] = new Integer(raf.readInt());
			}
			pid = (PageId) idConsts[0].newInstance(idArgs);

			Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
			int pageSize = raf.readInt();

			byte[] pageData = new byte[pageSize];
			raf.read(pageData); // read before image

			Object[] pageArgs = new Object[2];
			pageArgs[0] = pid;
			pageArgs[1] = pageData;

			newPage = (Page) pageConsts[0].newInstance(pageArgs);

			// Debug.printLogInfo("READ PAGE OF TYPE " + pageClassName + ",
			// table = " + newPage.getId().getTableId() + ", page = " +
			// newPage.getId().pageno());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException();
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new IOException();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new IOException();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			throw new IOException();
		}
		return newPage;

	}

	/**
	 * Write a BEGIN record for the specified transaction
	 * 
	 * @param tid
	 *            The transaction that is beginning
	 */
	public synchronized void logXactionBegin(TransactionId tid) throws IOException {
		Debug.printLogInfo("<START " + tid.getId() + ">");
		if (tidToFirstLogRecord.get(tid.getId()) != null) {
			System.err.printf("logXactionBegin: already began this tid\n");
			throw new IOException("double logXactionBegin()");
		}
		preAppend();
		raf.writeInt(BEGIN_RECORD);
		raf.writeLong(tid.getId());
		raf.writeLong(currentOffset);
		tidToFirstLogRecord.put(tid.getId(), currentOffset);
		currentOffset = raf.getFilePointer();

		Debug.printLogInfo("BEGIN OFFSET = " + currentOffset);
	}

	/** Checkpoint the log and write a checkpoint record. */
	public void logCheckpoint() throws IOException {
		// make sure we have buffer pool lock before proceeding
		synchronized (Database.getBufferPool()) {
			synchronized (this) {
				// Debug.printLogInfo("CHECKPOINT, offset = " +
				// raf.getFilePointer());
				preAppend();
				long startCpOffset, endCpOffset;
				Set<Long> keys = tidToFirstLogRecord.keySet();
				Iterator<Long> els = keys.iterator();
				force();

				// step 1Write to disk all the buffers that are dirty
				Database.getBufferPool().flushAllPages();

				// step2 Write a <START CKPT>record to the log, and flush the
				// log
				startCpOffset = raf.getFilePointer();
				raf.writeInt(CHECKPOINT_RECORD);
				raf.writeLong(-1); // no tid , but leave space for convenience

				/*
				 * -----------------------------------------------------
				 * |CHECKPOINT|-1|active_num|(tid, offset)*|startoffset|
				 * -----------------------------------------------------
				 */
				// write list of outstanding transactions
				raf.writeInt(keys.size());
				while (els.hasNext()) {
					Long key = els.next();
					Debug.printLogInfo("WRITING CHECKPOINT TRANSACTION ID: " + key);
					raf.writeLong(key);
					// Debug.printLogInfo("WRITING CHECKPOINT TRANSACTION
					// OFFSET: " + tidToFirstLogRecord.get(key));
					raf.writeLong(tidToFirstLogRecord.get(key));
				}

				// NOTE: once the CP is written, make sure the CP location at
				// the
				// beginning of the log file is updated
				endCpOffset = raf.getFilePointer();
				raf.seek(0);
				raf.writeLong(startCpOffset);
				raf.seek(endCpOffset);
				raf.writeLong(currentOffset);
				currentOffset = raf.getFilePointer();
				// Debug.printLogInfo("CP OFFSET = " + currentOffset);

				// step3 Write an <END CKPT> record to the log and flush the log

				// NOTE: Since we lock the BufferPool, so it's not possible that
				// there
				// are some other log record between <START CKPT> and <END
				// CKPT>, so we
				// don't need <END CKPT>

			}
		}

		logTruncate();
	}

	/**
	 * Truncate any unneeded portion of the log to reduce its space consumption
	 * 
	 * The log that Before the most early active transaction that in the CKPT
	 * can be truncate
	 */
	public synchronized void logTruncate() throws IOException {
		preAppend();
		raf.seek(0);

		// cpLoc the last checkpoint record start position
		long cpLoc = raf.readLong();

		long minLogRecord = cpLoc;

		if (cpLoc != -1L) {
			raf.seek(cpLoc);
			int cpType = raf.readInt();
			@SuppressWarnings("unused")
			long cpTid = raf.readLong();

			if (cpType != CHECKPOINT_RECORD) {
				throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
			}

			int numOutstanding = raf.readInt();// avtive tid num

			for (int i = 0; i < numOutstanding; i++) {
				@SuppressWarnings("unused")
				long tid = raf.readLong();
				long firstLogRecord = raf.readLong();
				if (firstLogRecord < minLogRecord) {
					minLogRecord = firstLogRecord;// find the most early
					// position
				}
			}
		}

		// we can truncate everything before minLogRecord
		File newFile = new File("logtmp" + System.currentTimeMillis());
		RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
		logNew.seek(0);
		logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

		raf.seek(minLogRecord);

		// have to rewrite log records since offsets are different after
		// truncation
		while (true) {
			try {
				int type = raf.readInt();
				long record_tid = raf.readLong();
				long newStart = logNew.getFilePointer();

				Debug.printLogInfo("NEW START = " + newStart);

				logNew.writeInt(type);
				logNew.writeLong(record_tid);

				switch (type) {
				case UPDATE_RECORD:
					Page before = readPageData(raf);
					Page after = readPageData(raf);

					writePageData(logNew, before);
					writePageData(logNew, after);
					break;
				case CHECKPOINT_RECORD:
					int numXactions = raf.readInt();
					logNew.writeInt(numXactions);
					while (numXactions-- > 0) {
						long xid = raf.readLong();
						long xoffset = raf.readLong();
						logNew.writeLong(xid);
						logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
					}
					break;
				case BEGIN_RECORD:
					tidToFirstLogRecord.put(record_tid, newStart);
					break;
				}

				// all actions finish with a pointer
				logNew.writeLong(newStart);
				raf.readLong();

			} catch (EOFException e) {
				break;
			}
		}

		Debug.printLogInfo("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord
				+ " NEW LENGTH: " + (raf.length() - minLogRecord));

		raf.close();
		logFile.delete();
		newFile.renameTo(logFile);
		raf = new RandomAccessFile(logFile, "rw");
		raf.seek(raf.length());

		newFile.delete();// TODO ?

		currentOffset = raf.getFilePointer();
		// print();
	}

	/**
	 * Rollback the specified transaction, setting the state of any of pages it
	 * updated to their pre-updated state. To preserve transaction semantics,
	 * this should not be called on transactions that have already committed
	 * (though this may not be enforced by this method.)
	 * 
	 * @param tid
	 *            The transaction to rollback
	 */
	public void rollback(TransactionId tid) throws NoSuchElementException, IOException {
		synchronized (Database.getBufferPool()) {
			synchronized (this) {
				preAppend();
				preAppend();
				// some code goes here

				// assert this tid is active
				// if null, the tid is already commit or abort
				Long isActive = this.tidToFirstLogRecord.get(tid.getId());
				if (isActive == null)
					return;

				long record_start = this.prevRecord(this.currentOffset);
				boolean isBegin = false;
				while (true) {
					switch (getRecordType(record_start))
					// fp+=4
					{
					case UPDATE_RECORD:
						// only fouce on tid
						if (tid.getId() != raf.readLong())// fp+=8
							break;
						Page pageq = this.readPageData(raf);
						// reset the fp to the position that before readpageData
						// 必须要seek fp的位置，在readPageData时fp位置改变
						raf.seek(record_start + 12);
						raf.readUTF();// String classname
						raf.readUTF();// String pidname
						raf.readInt();// int info_len
						int tableid = raf.readInt();
						int pgno = raf.readInt();
						HeapPageId pid = new HeapPageId(tableid, pgno);

						// file，file.writePage disk
						HeapFile file = (HeapFile) Database.getCatalog().getDbFile(tableid);

						file.writePage(pageq);

						// Page pagee = file.readPage(page.getId());

						Database.getBufferPool().pageMap.put(pid, pageq);

						break;
					case BEGIN_RECORD:
						long _tid = raf.readLong();
						/**
						 * When reach the BEGIN_RECORD of this transaction,
						 * rollback finished.
						 */
						if (_tid == tid.getId())
							isBegin = true;

						break;
					default:
						break;

					}
					if (isBegin)
						break;

					// get the prev record
					record_start = this.prevRecord(record_start);
				}

				raf.seek(currentOffset);

			}
		}
	}

	/**
	 * When the method return , fp will not point to the start
	 * 
	 * @param startPos
	 *            the start pos of the record
	 * @return the type of the record
	 */
	private int getRecordType(long startPos) {
		int type = -1;
		try {
			raf.seek(startPos);
			type = raf.readInt();

		} catch (IOException e) {
			e.printStackTrace();
		}

		return type;
	}

	/**
	 * Make the file point to the start of the record
	 * 
	 * @param nextStartPos
	 *            the start of next record, the end of the record we wanted
	 * @return the start pos of the prev record
	 */
	private long prevRecord(long nextStartPos) {
		long startPos = 0;
		try {
			raf.seek(nextStartPos - 8);
			startPos = raf.readLong();
			raf.seek(startPos);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return startPos;
	}

	/**
	 * Shutdown the logging system, writing out whatever state is necessary so
	 * that start up can happen quickly (without extensive recovery.)
	 */
	public synchronized void shutdown() {
		try {
			logCheckpoint(); // simple way to shutdown is to write a checkpoint
			// record
			raf.close();
		} catch (IOException e) {
			System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
			e.printStackTrace();
		}
	}

	/**
	 * Recover the database system by ensuring that the updates of committed
	 * transactions are installed and that the updates of uncommitted
	 * transactions are not installed.
	 */
	public void recover() throws IOException {
		synchronized (Database.getBufferPool()) {
			synchronized (this) {
				recoveryUndecided = false;
				// some code goes here
				LinkedList<LogR> redo = new LinkedList<LogR>();
				LinkedList<LogR> undo = new LinkedList<LogR>();
				HashSet<Long> redoSet = new HashSet<Long>();
				HashSet<Long> undoSet = new HashSet<Long>();
				long EOF = raf.length();
				raf.seek(0);
				long lastCKPT = raf.readLong();
				boolean hasCKPT = false;
				// start at 0
				while (raf.getFilePointer() != EOF) {
					int type = raf.readInt();
					long tid = raf.readLong();

					switch (type) {
					case BEGIN_RECORD:
						long pos = raf.readLong();// start position
						break;
					case ABORT_RECORD:
						/**
						 * If ABORT_RECORD appear in the log, the relevent
						 * transaction has already abort, no need redo or undo
						 * 
						 * @see logAbort()
						 */
						raf.readLong();// start position
						redoSet.remove(tid);
						undoSet.remove(tid);
						break;
					case COMMIT_RECORD:
						raf.readLong();// start position
						undoSet.remove(tid);
						redoSet.add(tid);
						break;
					case UPDATE_RECORD:
						undoSet.add(tid);
						Page before = readPageData(raf);// before data
						Page after = readPageData(raf);// after data
						undo.addFirst(new LogR(tid, before));
						redo.add(new LogR(tid, after));
						raf.readLong();
						break;
					case CHECKPOINT_RECORD:
						redoSet.clear();
						int numXactions = raf.readInt();
						while (numXactions-- > 0) {
							long xid = raf.readLong();
							long xoffset = raf.readLong();
						}
						raf.readLong();
						break;

					default:
						new util.Bug("error type");
					}
				}

				System.out.println("recoverSet done");

				// rodo
				for (LogR lr : redo) {
					long tid = lr.tid;
					if (!redoSet.contains(tid))
						continue;

					Page afterpage = lr.page;
					PageId pid = afterpage.getId();
					int tableid = pid.getTableId();
					HeapFile hfile = (HeapFile) Database.getCatalog().getDbFile(tableid);
					hfile.writePage(afterpage);

					Database.getBufferPool().pageMap.put(pid, afterpage);
				}

				// undo
				for (LogR lr : undo) {
					long tid = lr.tid;
					if (!undoSet.contains(tid))
						continue;

					Page beforepage = lr.page;
					PageId pid = beforepage.getId();
					int tableid = pid.getTableId();
					HeapFile hfile = (HeapFile) Database.getCatalog().getDbFile(tableid);
					hfile.writePage(beforepage);
					Database.getBufferPool().pageMap.put(pid, beforepage);
				}

			}
		}
	}

	/** Print out a human readable representation of the log */
	public void print() throws IOException {
		long curOffset = raf.getFilePointer();

		raf.seek(0);

		System.out.println("0: checkpoint record at offset " + raf.readLong());

		while (true) {
			try {
				int cpType = raf.readInt();
				long cpTid = raf.readLong();

				System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
				System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

				switch (cpType) {
				case BEGIN_RECORD:
					System.out.println(" (BEGIN)");
					System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
					break;
				case ABORT_RECORD:
					System.out.println(" (ABORT)");
					System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
					break;
				case COMMIT_RECORD:
					System.out.println(" (COMMIT)");
					System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
					break;

				case CHECKPOINT_RECORD:
					System.out.println(" (CHECKPOINT)");
					int numTransactions = raf.readInt();
					System.out.println(
							(raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

					while (numTransactions-- > 0) {
						long tid = raf.readLong();
						long firstRecord = raf.readLong();
						System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
						System.out.println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
					}
					System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

					break;
				case UPDATE_RECORD:
					System.out.println(" (UPDATE)");

					long start = raf.getFilePointer();
					Page before = readPageData(raf);

					long middle = raf.getFilePointer();
					Page after = readPageData(raf);

					System.out.println(start + ": before image table id " + before.getId().getTableId());
					System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().pageno());
					System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

					System.out.println(middle + ": after image table id " + after.getId().getTableId());
					System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().pageno());
					System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

					System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

					break;
				}

			} catch (EOFException e) {
				// e.printStackTrace();
				break;
			}
		}

		// Return the file pointer to its original position
		raf.seek(curOffset);
	}

	/**
	 * Forces any updates to this channel's file to be written to the storage
	 * device that contains it
	 * 
	 * @throws IOException
	 */
	public synchronized void force() throws IOException {
		raf.getChannel().force(true);
	}

	private class LogR {
		long tid;
		Page page;

		public LogR(long tid, Page page) {
			this.tid = tid;
			this.page = page;

		}
	}

}

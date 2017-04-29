package cbpdbms;

import java.io.*;

/**
 * Transaction encapsulates information about the state of a transaction and
 * manages transaction commit / abort.
 */

public class Transaction {
	TransactionId tid;
	boolean started = false;

	public Transaction() {
		tid = new TransactionId();
	}

	/**
	 * Start the transaction running
	 * 
	 * This instructs logging system to write a BEGIN record when a transaction
	 * starts.
	 */
	public void start() {
		started = true;
		try {
			Database.getLogFile().logXactionBegin(tid);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public TransactionId getId() {
		return tid;
	}

	/** Finish the transaction */
	public void commit() throws IOException {
		transactionComplete(false);
	}

	/**
	 * Handle the details of transaction commit / abort
	 * 
	 * This writes COMMIT or ABORT records to the log when a transaction ends.
	 * 
	 * @param abort
	 *            abort if true, commit if not
	 */
	public void transactionComplete(boolean abort) throws IOException {

		if (started) {
			// write commit / abort records
			if (abort) {
				Database.getLogFile().logAbort(tid); // does rollback too
			} else {
				// write all the dirty pages for this transaction out
				/*
				 * NOTE: A <COMMIT T> record must be flushed to disk as soon as
				 * it appears in the log
				 */
				Database.getBufferPool().flushPages(tid);
				Database.getLogFile().logCommit(tid);
			}

			try {
				// release locks
				Database.getBufferPool().transactionComplete(tid, !abort);

			} catch (IOException e) {
				e.printStackTrace();
			}

			// setting this here means we could possibly write multiple abort
			// records -- OK?
			started = false;
		}

	}

}

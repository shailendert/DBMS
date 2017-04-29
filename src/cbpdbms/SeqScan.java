package cbpdbms;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
	TransactionId tid;
	int tableid;
	String tableAlias;
	DbFileIterator it;

	/**
	 * Creates a sequential scan over the specified table as a part of the
	 * specified transaction.
	 *
	 * @param tid
	 *            The transaction this scan is running as a part of.
	 * @param tableid
	 *            the table to scan.
	 * @param tableAlias
	 *            the alias of this table (needed by the parser); the returned
	 *            tupleDesc should have fields with name tableAlias.fieldName
	 *            (note: this class is not responsible for handling a case where
	 *            tableAlias or fieldName are null. It shouldn't crash if they
	 *            are, but the resulting name can be null.fieldName,
	 *            tableAlias.null, or null.null).
	 * @throws DbException
	 * @throws NoSuchElementException
	 * @throws TransactionAbortedException
	 */
	public SeqScan(TransactionId tid, int tableid, String tableAlias)
			throws NoSuchElementException, DbException, TransactionAbortedException {
		// some code goes here
		this.tid = tid;
		this.tableid = tableid;
		this.tableAlias = tableAlias;
		this.it = Database.getCatalog().getDbFile(tableid).iterator(tid);
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		it.open();
	}

	/**
	 * Returns the TupleDesc with field names from the underlying HeapFile,
	 * prefixed with the tableAlias string from the constructor.
	 * 
	 * @return the TupleDesc with field names from the underlying HeapFile,
	 *         prefixed with the tableAlias string from the constructor.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		DbFile f = Database.getCatalog().getDbFile(tableid);
		int field_count = f.getTupleDesc().numFields();
		TupleDesc td = f.getTupleDesc();
		Type[] type = new Type[field_count];
		String[] name = new String[field_count];
		String prefix = "noali";

		if (this.tableAlias != null)
			prefix = this.tableAlias;

		for (int i = 0; i < field_count; i++) {
			String fieldname = "null";
			type[i] = td.getType(i);
			if (td.getFieldName(i) != null)
				fieldname = td.getFieldName(i);

			name[i] = prefix + "." + fieldname;
		}

		return new TupleDesc(type, name);
	}

	public boolean hasNext() throws TransactionAbortedException, DbException {
		// some code goes here
		return it.hasNext();
	}

	public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
		// some code goes here
		return it.next();
	}

	public void close() {
		// some code goes here
		it.close();
	}

	public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
		// some code goes here
		it.rewind();
	}
}

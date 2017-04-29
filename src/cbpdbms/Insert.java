package cbpdbms;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends AbstractDbIterator {
	TransactionId t;
	DbIterator child;
	int tableid;
	TupleDesc td;
	boolean founded;

	/**
	 * Constructor.
	 * 
	 * @param t
	 *            The transaction running the insert.
	 * @param child
	 *            The child operator from which to read tuples to be inserted.
	 * @param tableid
	 *            The table in which to insert tuples.
	 * @throws DbException
	 *             if TupleDesc of child differs from table into which we are to
	 *             insert.
	 */
	public Insert(TransactionId t, DbIterator child, int tableid) throws DbException {
		// some code goes here
		this.t = t;
		this.child = child;
		this.tableid = tableid;
		this.founded = false;

		Type[] type = new Type[] { Type.INT_TYPE };
		String[] name = new String[] { "count" };
		this.td = new TupleDesc(type, name);

		if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid)))
			throw new DbException("tupleDesc of child differs from table into which tableid represent");
	}

	public TupleDesc getTupleDesc() {
		// some code goes here
		return td;
	}

	public void open() throws DbException, TransactionAbortedException {
		// some code goes here
		child.open();
	}

	public void close() {
		// some code goes here
		child.close();
		super.close();
	}

	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		child.rewind();
	}

	/**
	 * Inserts tuples read from child into the tableid specified by the
	 * constructor. It returns a one field tuple containing the number of
	 * inserted records. Inserts should be passed through BufferPool. An
	 * instances of BufferPool is available via Database.getBufferPool(). Note
	 * that insert DOES NOT need check to see if a particular tuple is a
	 * duplicate before inserting it.
	 *
	 * @return A 1-field tuple containing the number of inserted records, or
	 *         null if called more than once.
	 * @see Database#getBufferPool
	 * @see BufferPool#insertTuple
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		// some code goes here
		Tuple tup = new Tuple(td);
		int count = 0;

		if (founded)
			return null;
		else {
			founded = true;
			while (child.hasNext()) {
				Tuple t = child.next();
				try {
					Database.getBufferPool().insertTuple(this.t, tableid, t);
				} catch (IOException e) {
					e.printStackTrace();
				}
				count++;
			}
			Field field = new IntField(count);
			tup.setField(0, field);
		}

		return tup;

	}
}

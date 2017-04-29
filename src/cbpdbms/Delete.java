package cbpdbms;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
	TransactionId t;
	DbIterator child;

	TupleDesc td;// the return Tuple's desc
	boolean fetched;

	/**
	 * Constructor specifying the transaction that this delete belongs to as
	 * well as the child to read from.
	 * 
	 * @param t
	 *            The transaction this delete runs in
	 * @param child
	 *            The child operator from which to read tuples for deletion
	 */
	public Delete(TransactionId t, DbIterator child) {
		// some code goes here
		this.t = t;
		this.child = child;

		Type[] type = new Type[] { Type.INT_TYPE };
		String[] name = new String[] { "count" };
		this.td = new TupleDesc(type, name);
		this.fetched = false;
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
	 * Deletes tuples as they are read from the child operator. Deletes are
	 * processed via the buffer pool (which can be accessed via the
	 * Database.getBufferPool() method.
	 * 
	 * @return A 1-field tuple containing the number of deleted records.
	 * @see Database#getBufferPool
	 * @see BufferPool#deleteTuple
	 */
	protected Tuple readNext() throws TransactionAbortedException, DbException {
		// some code goes here
		int count = 0;
		
		if (fetched)
			return null;
		Tuple tuple = new Tuple(td);
		while (child.hasNext()) {

			Tuple tup = child.next();
			Database.getBufferPool().deleteTuple(t, tup);
			count++;
		}

		Field ifield = new IntField(count);
		tuple.setField(0, ifield);
		fetched = true;
		return tuple;
	}
}

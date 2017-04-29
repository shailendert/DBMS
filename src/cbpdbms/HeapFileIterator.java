package cbpdbms;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	TransactionId tid;
	Iterator<Tuple> it;
	List<Tuple> list;

	public HeapFileIterator(TransactionId tid, List<Tuple> list) {
		this.tid = tid;
		this.list = list;
	}

	@Override
	public void open() throws DbException, TransactionAbortedException {
		it = this.list.iterator();

	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (it == null)
			return false;

		return it.hasNext();
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		if (it == null)
			throw new NoSuchElementException("tuple is null");

		return it.next();
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		close();
		open();

	}

	@Override
	public void close() {
		it = null;
	}

}

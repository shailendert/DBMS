package cbpdbms;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;

	TupleDesc td;
	List<Tuple> list;
	Map<Field, Tuple> map;
	Map<Field, Integer> avg_map;// for case:avg, case: count

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException
	 *             if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		this.list = new LinkedList<Tuple>();
		this.map = new HashMap<Field, Tuple>();
		this.avg_map = new HashMap<Field, Integer>();

		// TupleDesc(Unknown, INT),(Unknown, STRING)
		if (gbfieldtype == null) {
			Type[] type = { Type.INT_TYPE };
			this.td = new TupleDesc(type);
		} else {
			Type[] type = { gbfieldtype, Type.INT_TYPE };
			this.td = new TupleDesc(type);
		}
	}

	/**
	 * Tuple，IntField1
	 * 
	 * @param tup
	 * @return
	 */
	private Tuple createTuple(Tuple tup) {
		Tuple t = new Tuple(td);
		if (this.gbfieldtype == null) {
			t.setField(0, new IntField(1));
		} else {
			t.setField(0, tup.getField(gbfield));
			t.setField(1, new IntField(1));
		}
		return t;
	}

	/**
	 * for the mergeCount. make the tuple's field +1.
	 * 
	 * @param tup
	 * 
	 */
	private void addcount(Tuple tup) {
		if (this.gbfieldtype == null) {
			IntField ifield = (IntField) tup.getField(0);
			ifield.setValue(ifield.getValue() + 1);
		} else {
			IntField ifield = (IntField) tup.getField(1);
			ifield.setValue(ifield.getValue() + 1);
		}

	}

	private void mergeCount(Tuple t) {
		Tuple tt = createTuple(t);
		Field gbfield = tt.getField(0);
		if (map.containsKey(gbfield)) {// 分组已经存在
			Tuple tup = map.get(gbfield);
			addcount(tup);
		} else {
			map.put(tt.getField(0), tt);
			list.add(tt);
		}

	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void merge(Tuple tup) {
		// some code goes here
		switch (this.what) {
		case COUNT:
			mergeCount(tup);
			break;
		default:
			;
		}
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		return new StringAggregatorIterator(list);
	}

	public static class StringAggregatorIterator implements DbIterator {
		List<Tuple> list;
		Iterator<Tuple> it;

		public StringAggregatorIterator(List<Tuple> list) {
			this.list = list;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			it = list.iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			return it.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			return it.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
		}

		@Override
		public TupleDesc getTupleDesc() {
			Tuple t = list.get(0);
			return t.getTupleDesc();
		}

		@Override
		public void close() {
			it = null;
		}

	}

}

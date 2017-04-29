package cbpdbms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
	int gbfield;
	Type gbfieldtype;
	int afield;
	Op what;

	TupleDesc td;
	List<Tuple> list;
	Map<Field, Tuple> map;// for groupby
	Map<Field, ArrayList<Integer>> avg_map;// for case:avg(groupby)
	ArrayList<Integer> avg_list; // for no groupby

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
	 *            the aggregation operator
	 */

	public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		this.list = new LinkedList<Tuple>();
		this.map = new HashMap<Field, Tuple>();
		this.avg_map = new HashMap<Field, ArrayList<Integer>>();
		this.avg_list = new ArrayList<Integer>();

		if (gbfieldtype == null) {
			Type[] type = { Type.INT_TYPE };
			this.td = new TupleDesc(type);
		} else {
			Type[] type = { gbfieldtype, Type.INT_TYPE };
			this.td = new TupleDesc(type);
		}
	}

	/**
	 * according to the given Tuple, create a new Tuple that has two Filed or
	 * one Field
	 * 
	 * @param tup
	 * @return
	 */
	private Tuple createTuple(Tuple tup) {
		Tuple t = new Tuple(td);
		if (this.gbfieldtype == null) {
			t.setField(0, tup.getField(afield));
		} else {
			t.setField(0, tup.getField(gbfield));
			t.setField(1, tup.getField(afield));
		}
		return t;
	}

	private void mergeMin(Tuple t) {
		Tuple tt = createTuple(t);
		Field gbfield = tt.getField(0);
		if (map.containsKey(gbfield)) {// 分组已经存在
			Tuple old = map.get(gbfield);
			IntField oldifield = getAggField(old);
			IntField curr = (IntField) t.getField(afield);
			if (curr.getValue() < oldifield.getValue())
				oldifield.setValue(curr.getValue());
		} else {
			map.put(gbfield, tt);
			list.add(tt);
		}

	}

	private void mergeMax(Tuple t) {
		Tuple tt = createTuple(t);
		Field gbfield = tt.getField(0);
		if (map.containsKey(gbfield)) {
			Tuple old = map.get(gbfield);
			IntField oldifield = getAggField(old);
			IntField curr = (IntField) t.getField(afield);
			if (curr.getValue() > oldifield.getValue())
				oldifield.setValue(curr.getValue());
		} else {
			map.put(gbfield, tt);
			list.add(tt);
		}

	}

	/**
	 * calculate the avg of the given arraylist
	 * 
	 * @param list
	 * @return
	 */
	private int calculateAvg(ArrayList<Integer> list) {
		int count = 0;
		int avg = 0;
		for (Integer i : list) {
			count++;
			avg += i;
		}

		avg = avg / count;
		return avg;
	}

	/**
	 * mergeAvg case1: groupBy case2: no groupBy
	 * 
	 * @param t
	 */
	private void mergeAvg(Tuple t) {
		Tuple tt = createTuple(t);
		// System.out.println(tt);

		if (this.gbfieldtype != null) {			// group by

			Field gbfield = tt.getField(0);
			if (map.containsKey(gbfield)) {			// map tuple
				Tuple old = map.get(gbfield);
				IntField oldifield = getAggField(old);
				IntField curr = (IntField) t.getField(afield);
				// afield list
				ArrayList<Integer> afieldList = avg_map.get(gbfield);
				afieldList.add(curr.getValue());
				oldifield.setValue(calculateAvg(afieldList));
			} else {
				map.put(gbfield, tt);
				list.add(tt);
				avg_map.put(gbfield, new ArrayList<Integer>());

				ArrayList<Integer> afieldList = avg_map.get(gbfield);
				afieldList.add(getAggField(tt).getValue());

			}
		} else {// no group by
			if (list.isEmpty()) {
				list.add(tt);
				IntField ifield = (IntField) tt.getField(0);
				avg_list.add(ifield.getValue());
			} else {
				Tuple old = list.get(0);
				// afield list
				IntField curr = (IntField) tt.getField(0);
				avg_list.add(curr.getValue());
				// avg
				IntField oldifield = getAggField(old);
				oldifield.setValue(calculateAvg(avg_list));
			}
		}

	}

	/**
	 * for the mergeCount. make the tuple's field +1.
	 * 
	 * @param tup
	 */
	private void addcount(Tuple tup) {
		IntField ifield = (IntField) getAggField(tup);
		ifield.setValue(ifield.getValue() + 1);
	}

	/**
	 * for the mergeCount. If case:COUNT, the field should init with 1
	 * 
	 * @param tuple
	 */
	private void resetTuple(Tuple tuple) {
		if (this.gbfieldtype == null) {
			IntField ifield = (IntField) tuple.getField(0);
			ifield.setValue(1);
		} else {
			IntField ifield = (IntField) tuple.getField(1);
			ifield.setValue(1);

		}

	}

	private void mergeCount(Tuple t) {
		Tuple tt = createTuple(t);
		Field gbfield = tt.getField(0);
		if (map.containsKey(gbfield)) {
			Tuple tup = map.get(gbfield);
			addcount(tup);
		} else {
			map.put(tt.getField(0), tt);
			resetTuple(tt);
			list.add(tt);

		}

	}

	/**
	 * 
	 * @param t
	 */
	private void mergeSum(Tuple t) {
		Tuple tt = createTuple(t);
		if (map.containsKey(tt.getField(0))) {
			Tuple old = map.get(tt.getField(0));
			IntField ifield = getAggField(old);
			IntField curr = (IntField) t.getField(afield);
			ifield.setValue(ifield.getValue() + curr.getValue());
		} else {
			map.put(tt.getField(0), tt);
			list.add(tt);
		}

	}

	/**
	 * according to the gbfieldtype, get the field which we want to change
	 * 
	 * @param t
	 * @return
	 */
	public IntField getAggField(Tuple t) {
		IntField ifield;

		if (this.gbfieldtype == null)
			ifield = (IntField) t.getField(0);
		else
			ifield = (IntField) t.getField(1);
		return ifield;
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
		case MIN:
			mergeMin(tup);
			break;
		case MAX:
			mergeMax(tup);
			break;
		case AVG:
			mergeAvg(tup);
			break;
		case SUM:
			mergeSum(tup);
			break;
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
		return new IntAggregatorIterator(list);
	}

	public static class IntAggregatorIterator implements DbIterator {
		List<Tuple> list;
		Iterator<Tuple> it;

		public IntAggregatorIterator(List<Tuple> list) {
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

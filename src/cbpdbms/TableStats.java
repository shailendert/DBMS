package cbpdbms;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query
 */
public class TableStats {
	/**
	 * Number of bins for the histogram. Feel free to increase this value over
	 * 100, though our tests assume that you have at least 100 bins in your
	 * histograms.
	 */
	static final int NUM_HIST_BINS = 100;
	int tiableid;
	int ioCostPerPage;
	DbFile file;
	TransactionId tid;
	int ntups;
	DbFileIterator it;
	TupleDesc td;

	/**
	 * Every column is a histogram.
	 * 
	 * Integer is the index of the column in the TupleDesc of the table
	 */
	Map<Integer, IntHistogram> imap;
	Map<Integer, StringHistogram> smap;

	/**
	 * The first time scan the table should calculate the max value and min
	 * value for all column
	 * 
	 * Integer is the index of the column in the TupleDesc of the table
	 */
	Map<Integer, Integer> maxmap;
	Map<Integer, Integer> minmap;

	/**
	 * Create a new TableStats object, that keeps track of statistics on each
	 * columnn of a table
	 * 
	 * @param tableid
	 *            The table over which to compute statistics
	 * @param ioCostPerPage
	 *            The cost per page of IO. This doesn't differentiate between
	 *            sequential-scan IO and disk seeks.
	 * @throws TransactionAbortedException
	 * @throws DbException
	 */
	public TableStats(int tableid, int ioCostPerPage) {
		// For this function, you'll have to get the DbFile for the table in
		// question,k
		// then scan through its tuples and calculate the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily
		// have to (for example) do everything in a single scan of the table.
		// some code goes here
		this.tiableid = tableid;
		this.ioCostPerPage = ioCostPerPage;
		this.file = Database.getCatalog().getDbFile(tableid);
		this.imap = new HashMap<Integer, IntHistogram>();
		this.smap = new HashMap<Integer, StringHistogram>();
		this.maxmap = new HashMap<Integer, Integer>();
		this.minmap = new HashMap<Integer, Integer>();

		td = file.getTupleDesc();
		initMaxminMap(td);

		Transaction tran = new Transaction();
		tran.start();
		this.tid = tran.getId();
		try {
			this.it = file.iterator(tid);
			it.open();

			// step1 calculate the max and min for int column
			defineMaxMin(it);

			it.rewind();

			// step2 init histogram
			initHistogram();

			// step3 add value to coresponding histogram
			establishHistogram(it);

			it.close();

			tran.commit();

		} catch (DbException | TransactionAbortedException | IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * scan the table, and fill all the histogram
	 * 
	 * @param it
	 * @throws DbException
	 * @throws TransactionAbortedException
	 */
	private void establishHistogram(DbFileIterator it) throws DbException, TransactionAbortedException {

		int column = td.numFields();
		while (it.hasNext()) {
			Tuple tup = it.next();

			for (int i = 0; i < column; i++) {
				Type t = tup.getField(i).getType();
				switch (t) {
				case INT_TYPE:
					IntField ifield = (IntField) tup.getField(i);
					IntHistogram ihis = this.imap.get(i);
					ihis.addValue(ifield.getValue());

					this.imap.put(i, ihis);
					break;
				case STRING_TYPE:
					StringField sfield = (StringField) tup.getField(i);
					StringHistogram shis = this.smap.get(i);
					shis.addValue(sfield.getValue());
					this.smap.put(i, shis);
					break;

				default:
					new util.Bug("wrong type");
				}
			}
		}
	}

	/**
	 * according to the MaxMap and MinMap, init the Histogram
	 * 
	 * @param td
	 */
	private void initHistogram() {
		int column = td.numFields();
		for (int i = 0; i < column; i++) {
			Type t = td.getType(i);
			switch (t) {
			case INT_TYPE:

				int max = this.maxmap.get(i);
				int min = this.minmap.get(i);
				IntHistogram ihis = new IntHistogram(NUM_HIST_BINS, min, max);
				this.imap.put(i, ihis);
				break;
			case STRING_TYPE:
				this.smap.put(i, new StringHistogram(NUM_HIST_BINS));
				break;
			default:
				new util.Bug("wrong type");
			}
		}
	}

	/**
	 * define the max and min of every columnn and calculate the ntups.
	 * 
	 * @param tup
	 * @throws TransactionAbortedException
	 * @throws DbException
	 * @throws NoSuchElementException
	 */
	private void defineMaxMin(DbFileIterator it)
			throws NoSuchElementException, DbException, TransactionAbortedException {
		while (it.hasNext()) {
			Tuple tup = it.next();
			int column = tup.getTupleDesc().numFields();
			for (int i = 0; i < column; i++) {
				Type t = tup.getTupleDesc().getType(i);
				switch (t) {
				case INT_TYPE:
					IntField ifield = (IntField) tup.getField(i);
					int value = ifield.getValue();
					if (value > this.maxmap.get(i))
						maxmap.put(i, value);
					if (value < this.minmap.get(i))
						minmap.put(i, value);
					break;
				case STRING_TYPE:
					break;
				default:
					new util.Bug("wrong type");
				}
			}
			this.ntups++;
		}

	}

	/**
	 * init the map of Max and Min
	 * 
	 * @param td
	 */
	private void initMaxminMap(TupleDesc td) {
		int column = td.numFields();
		// init the maxmap, and minmap
		for (int i = 0; i < column; i++) {
			Type t = td.getType(i);
			switch (t) {
			case INT_TYPE:
				maxmap.put(i, Integer.MIN_VALUE);
				minmap.put(i, Integer.MAX_VALUE);
				break;
			case STRING_TYPE:
				break;

			default:
				new util.Bug("wrong type");
			}
		}
	}

	/**
	 * Estimates the cost of sequentially scanning the file, given that the cost
	 * to read a page is costPerPageIO. You can assume that there are no seeks
	 * and that no pages are in the buffer pool.
	 * 
	 * Also, assume that your hard drive can only read entire pages at once, so
	 * if the last page of the table only has one tuple on it, it's just as
	 * expensive to read as a full page. (Most real hard drives can't
	 * efficiently address regions smaller than a page at a time.)
	 * 
	 * @return The estimated cost of scanning the table.
	 */
	public double estimateScanCost() {
		// some code goes here
		return this.file.numPages() * this.ioCostPerPage;
	}

	/**
	 * This method returns the number of tuples in the relation, given that a
	 * predicate with selectivity selectivityFactor is applied.
	 *
	 * @param selectivityFactor
	 *            The selectivity of any predicates over the table
	 * @return The estimated cardinality of the scan with the specified
	 *         selectivityFactor
	 */
	public int estimateTableCardinality(double selectivityFactor) {
		// return (int) (this.ntups * selectivityFactor);
		return (int) Math.ceil(ntups * selectivityFactor);
	}

	/**
	 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
	 * table.
	 * 
	 * @param field
	 *            The field over which the predicate ranges
	 * @param op
	 *            The logical operation in the predicate
	 * @param constant
	 *            The value against which the field is compared
	 * @return The estimated selectivity (fraction of tuples that satisfy) the
	 *         predicate
	 */
	public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
		// some code goes here
		// String name = td.getFieldName(field);
		Type t = constant.getType();
		switch (t) {
		case INT_TYPE:
			IntHistogram ihis = this.imap.get(field);
			IntField ifield = (IntField) constant;
			return ihis.estimateSelectivity(op, ifield.getValue());
		case STRING_TYPE:
			StringHistogram shis = this.smap.get(field);
			StringField sfield = (StringField) constant;
			return shis.estimateSelectivity(op, sfield.getValue());

		default:
			new util.Bug("wrong type");
		}

		return 1.0;
	}
}

package simpledb.systemtest;

import java.io.IOException;

import cbpdbms.DbException;
import cbpdbms.Filter;
import cbpdbms.HeapFile;
import cbpdbms.Predicate;
import cbpdbms.SeqScan;
import cbpdbms.TransactionAbortedException;
import cbpdbms.TransactionId;
import simpledb.*;

import static org.junit.Assert.*;

public class FilterTest extends FilterBase {
	@Override
	protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
			throws DbException, TransactionAbortedException, IOException {
		SeqScan ss = new SeqScan(tid, table.getId(), "");
		Filter filter = new Filter(predicate, ss);
		filter.open();

		int resultCount = 0;
		while (filter.hasNext()) {
			assertNotNull(filter.next());
			resultCount += 1;
		}

		filter.close();
		return resultCount;
	}

	/** Make test compatible with older version of ant. */
	public static junit.framework.Test suite() {
		return new junit.framework.JUnit4TestAdapter(FilterTest.class);
	}
}

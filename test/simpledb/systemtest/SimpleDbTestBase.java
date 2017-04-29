package simpledb.systemtest;

import org.junit.Before;

import cbpdbms.Database;

/**
 * Base class for all SimpleDb test classes.
 */
public class SimpleDbTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before
	public void setUp() throws Exception {
		Database.reset();
	}

}

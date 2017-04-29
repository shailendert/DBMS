package cbpdbms;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple {
	private TupleDesc td;
	private Field[] field;
	private RecordId rid;// tuple slot

	/**
	 * Create a new tuple with the specified schema (type).
	 *
	 * @param td
	 *            the schema of this tuple. It must be a valid TupleDesc
	 *            instance with at least one field.
	 */
	public Tuple(TupleDesc td) {
		this.td = td;
		this.field = new Field[td.numFields()];
	}

	/**
	 * @return The TupleDesc representing the schema of this tuple.
	 */
	public TupleDesc getTupleDesc() {
		return this.td;
	}

	/**
	 * @return The RecordId representing the location of this tuple on disk. May
	 *         be null.
	 */
	public RecordId getRecordId() {
		// some code goes here
		return this.rid;
	}

	/**
	 * Set the RecordId information for this tuple.
	 * 
	 * @param rid
	 *            the new RecordId for this tuple.
	 */
	public void setRecordId(RecordId rid) {
		// some code goes here
		this.rid = rid;
	}

	/**
	 * Change the value of the ith field of this tuple.
	 *
	 * @param i
	 *            index of the field to change. It must be a valid index.
	 * @param f
	 *            new value for the field.
	 */
	public void setField(int i, Field f) {
		// some code goes here
		this.field[i] = f;
	}

	/**
	 * @return the value of the ith field, or null if it has not been set.
	 *
	 * @param i
	 *            field index to return. Must be a valid index.
	 */
	public Field getField(int i) {
		// some code goes here
		// return null;
		return this.field[i];
	}

	/**
	 * Returns the contents of this Tuple as a string. Note that to pass the
	 * system tests, the format needs to be as follows:
	 *
	 * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
	 *
	 * where \t is any whitespace, except newline, and \n is a newline
	 */
	public String toString() {
		// some code goes here
		String s = "";
		String tab = "";
		for (Field f : this.field) {
			s += tab + f.toString();
			tab = "\t";
		}
		s += "\n";
		return s;
		// throw new UnsupportedOperationException("Implement this");
	}

	/**
	 * combine two tuples simply. no need to remove the duplicate element
	 * 
	 * @param t1
	 *            the first Tuple
	 * @param t2
	 *            the Tuple behind t1
	 * @return rew Tuple that t1[0]|t1[1]...t1[end]|t2[0]|t2[1]|...t2[end]
	 */
	public static Tuple simpleCombine(Tuple t1, Tuple t2) {
		TupleDesc td = TupleDesc.combine(t1.getTupleDesc(), t2.getTupleDesc());

		Tuple t = new Tuple(td);

		int len1 = t1.getTupleDesc().numFields();
		int len2 = t2.getTupleDesc().numFields();
		int i = 0;
		for (int j = 0; j < len1; i++, j++)
			t.setField(i, t1.getField(j));

		for (int j = 0; j < len2; i++, j++)
			t.setField(i, t2.getField(j));
		// no need recordId
		return t;
	}
}

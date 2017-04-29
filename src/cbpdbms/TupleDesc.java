package cbpdbms;

import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	private Type[] typeAr;
	private String[] fieldAr;

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
	 * with the first td1.numFields coming from td1 and the remaining from td2.
	 * 
	 * @param td1
	 *            The TupleDesc with the first fields of the new TupleDesc
	 * @param td2
	 *            The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
		// some code goes here
		int new_size = td1.numFields() + td2.numFields();
		Type[] ntype = new Type[new_size];
		String[] nfield = new String[new_size];
		int i = 0;
		for (Type t : td1.typeAr)
			ntype[i++] = t;
		for (Type t : td2.typeAr)
			ntype[i++] = t;
		i = 0;

		// NOTE:td1 td fieldAr
		if (td1.fieldAr != null)
			for (String s : td1.fieldAr)
				nfield[i++] = s;
		else
			for (Type t : td1.typeAr)
				nfield[i++] = "unknowname";
		if (td2.fieldAr != null)
			for (String s : td2.fieldAr)
				nfield[i++] = s;
		else
			for (Type t : td2.typeAr)
				nfield[i++] = "unknowname";
		return new TupleDesc(ntype, nfield);
	}

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the
	 * specified types, with associated named fields.
	 *
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 * @param fieldAr
	 *            array specifying the names of the fields. Note that names may
	 *            be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		// some code goes here
		this.typeAr = typeAr;
		this.fieldAr = fieldAr;

	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with
	 * fields of the specified types, with anonymous (unnamed) fields.
	 *
	 * @param typeAr
	 *            array specifying the number of and types of fields in this
	 *            TupleDesc. It must contain at least one entry.
	 */
	public TupleDesc(Type[] typeAr) {
		// some code goes here
		this.typeAr = typeAr;
		// String[] fieldAr = new String[typeAr.length];

		this.fieldAr = new String[typeAr.length];

		for (int i = 0; i < fieldAr.length; i++)
			fieldAr[i] = "unname";
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		return this.typeAr.length;
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 *
	 * @param i
	 *            index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		if (this.fieldAr == null)
			return null;
		else
			return this.fieldAr[i];
	}

	/**
	 * Find the index of the field with a given name.
	 *
	 * @param name
	 *            name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException
	 *             if no field with a matching name is found.
	 */
	public int nameToId(String name) throws NoSuchElementException {
		// String[] s = name.split("\\.");
		//
		// if (s.length == 2)
		// name = s[1];

		if (this.fieldAr == null)
			throw new NoSuchElementException();
		else {
			for (int i = 0; i < this.fieldAr.length; i++) {
				if (!this.fieldAr[i].equals(name))
					continue;
				return i;
			}
			throw new NoSuchElementException();
		}
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 *
	 * @param i
	 *            The index of the field to get the type of. It must be a valid
	 *            index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public Type getType(int i) throws NoSuchElementException {
		return this.typeAr[i];
	}

	/**
	 * @return The size (in bytes) of tuples corresponding to this TupleDesc.
	 *         Note that tuples from a given TupleDesc are of a fixed size.
	 */
	public int getSize() {
		int size = 0;
		for (Type t : this.typeAr)
			size += t.getLen();
		return size;
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two
	 * TupleDescs are considered equal if they are the same size and if the n-th
	 * type in this TupleDesc is equal to the n-th type in td.
	 *
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {
		if (o == null)
			return false;

		if (!(o instanceof TupleDesc))
			return false;

		TupleDesc tdd = (TupleDesc) o;
		if (this.getSize() != tdd.getSize())
			return false;

		for (int i = 0; i < this.typeAr.length; i++) {
			if (this.typeAr[i].equals(tdd.typeAr[i]))
				continue;

			return false;
		}
		return true;
	}

	public int hashCode() {
		// If you want to use TupleDesc as keys for HashMap, implement this so
		// that equal objects have equals hashCode() results
		throw new UnsupportedOperationException("unimplemented");
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
	 * the exact format does not matter.
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// some code goes here
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < this.fieldAr.length; i++) {
			sb.append(this.fieldAr[i]);
			sb.append("(");
			sb.append(this.typeAr[i]);
			sb.append(")\n");
		}
		return sb.toString();
	}
}

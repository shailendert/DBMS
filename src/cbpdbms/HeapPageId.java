package cbpdbms;

import cbpdbms.PageSharedExclusiveLock.ExclusiveLock;
import cbpdbms.PageSharedExclusiveLock.SharedLock;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

	int tableid;// hashcode
	int pgNo;

	/**
	 * We choose page as the db element
	 */
	PageSharedExclusiveLock lock;
	SharedLock sharedLock;
	ExclusiveLock exclusiveLock;

	/**
	 * Constructor. Create a page id structure for a specific page of a specific
	 * table.
	 *
	 * @param tableId
	 *            The table that is being referenced
	 * @param pgNo
	 *            The page number in that table.
	 */
	public HeapPageId(int tableId, int pgNo) {
		this.tableid = tableId;// file's hashcode
		this.pgNo = pgNo;
		this.lock = new PageSharedExclusiveLock();
		this.sharedLock = lock.sharedLock;
		this.exclusiveLock = lock.exclusiveLock;
	}

	/** @return the table associated with this PageId */
	public int getTableId() {
		return this.tableid;
	}

	/**
	 * @return the page number in the table getTableId() associated with this
	 *         PageId
	 */
	public int pageno() {
		return this.pgNo;
	}

	/**
	 * @return a hash code for this page, represented by the concatenation of
	 *         the table number and the page number (needed if a PageId is used
	 *         as a key in a hash table in the BufferPool, for example.)
	 * @see BufferPool
	 */
	public int hashCode() {
		String s1 = String.valueOf(tableid);
		String s2 = String.valueOf(pgNo);
		String s = s1 + s2;
		Long l = Long.parseLong(s);
		int ihashcode = l.hashCode();
		return ihashcode;
	}

	/**
	 * Compares one PageId to another.
	 *
	 * @param o
	 *            The object to compare against (must be a PageId)
	 * @return true if the objects are equal (e.g., page numbers and table ids
	 *         are the same)
	 */
	public boolean equals(Object o) {
		if (!(o instanceof PageId))
			return false;

		PageId p = (PageId) o;
		if ((this.tableid == p.getTableId()) && (this.pgNo == p.pageno()))
			return true;

		return false;
	}

	/**
	 * Return a representation of this object as an array of integers, for
	 * writing to disk. Size of returned array must contain number of integers
	 * that corresponds to number of args to one of the constructors.
	 */
	public int[] serialize() {
		int data[] = new int[2];

		data[0] = getTableId();
		data[1] = pageno();

		return data;
	}

	public String toString() {
		return String.valueOf(hashCode());
	}

}

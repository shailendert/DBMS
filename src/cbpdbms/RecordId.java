package cbpdbms;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {
	PageId pid;// record
	/**
	 * indicate the tuple in which solt of a Page
	 */
	int tupleno;

	/**
	 * Creates a new RecordId refering to the specified PageId and tuple number.
	 * 
	 * @param pid
	 *            the pageid of the page on which the tuple resides
	 * @param tupleno
	 *            the tuple number within the page.
	 */
	public RecordId(PageId pid, int tupleno) {
		this.pid = pid;
		this.tupleno = tupleno;
	}

	/**
	 * @return the tuple number this RecordId references.
	 */
	public int tupleno() {
		// some code goes here
		return this.tupleno;
	}

	/**
	 * @return the page id this RecordId references.
	 */
	public PageId getPageId() {
		return this.pid;
	}

	/**
	 * Two RecordId objects are considered equal if they represent the same
	 * tuple.
	 * 
	 * @return True if this and o represent the same tuple
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof RecordId))
			return false;

		RecordId rid = (RecordId) o;
		if (this.pid.equals(rid.pid) && this.tupleno == rid.tupleno)
			return true;
		else
			return false;

	}

	/**
	 * You should implement the hashCode() so that two equal RecordId instances
	 * (with respect to equals()) have the same hashCode().
	 * 
	 * Since we override hashcode, maybe we can implements lock in recordId
	 * 
	 * @return An int that is the same for equal RecordId objects.
	 */
	@Override
	public int hashCode() {
		// some code goes here
		int i = Integer.parseInt(String.valueOf(tupleno)) + pid.hashCode();
		return i;

	}

}

package cbpdbms;

import cbpdbms.Predicate.Op;

/**
 * A class to represent a fixed-range histogram over a single integer-based
 * field.
 */
public class IntHistogram {
	BucketItem bucket[];
	int buckets;
	int min;
	int max;
	int ntups;
	double width;
	double range;

	public static class BucketItem {
		double right;
		double left;
		double part;
		int height;

		public BucketItem(double left, double right) {
			this.left = left;
			this.right = right;
			this.part = 0;
			this.height = 0;
		}

		public double getRightPart(int v) {
			double rpart;
			rpart = (this.right - v) / (this.right - this.left);
			return rpart * part;
		}

		public double getLeftPart(int v) {
			double rpart;
			rpart = (this.right - v) / (this.right - this.left);
			return rpart * part;
		}
	}

	/**
	 * Create a new IntHistogram.
	 * 
	 * This IntHistogram should maintain a histogram of integer values that it
	 * receives. It should split the histogram into "buckets" buckets.
	 * 
	 * The values that are being histogrammed will be provided one-at-a-time
	 * through the "addValue()" function.
	 * 
	 * Your implementation should use space and have execution time that are
	 * both constant with respect to the number of values being histogrammed.
	 * For example, you shouldn't simply store every value that you see in a
	 * sorted list.
	 * 
	 * @param buckets
	 *            The number of buckets to split the input value into.
	 * @param min
	 *            The minimum integer value that will ever be passed to this
	 *            class for histogramming
	 * @param max
	 *            The maximum integer value that will ever be passed to this
	 *            class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		// some code goes here
		this.buckets = buckets;
		this.min = min;
		this.max = max;
		this.ntups = 0;
		this.bucket = new BucketItem[buckets];
		this.range = (double) (max - min) / buckets;

		/*
		 * For equale estimate. if range is 0.1, then the height/range is not
		 * correct.
		 * 
		 * If we use (height/ntups)/height, the buckets must as large is
		 * possible to make result precise. So we use ceil(rang) as a rough
		 * estimation
		 */
		this.width = Math.ceil(range);

		for (int i = 0; i < this.bucket.length; i++) {
			this.bucket[i] = new BucketItem(min + i * range, min + (i + 1) * range);
		}

	}

	private int calculateIdx(int v) {
		int idx = (int) ((v - min) / range);

		if (v == max)
			idx--;
		return idx;
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * 
	 * @param v
	 *            Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here
		int idx = calculateIdx(v);
		ntups++;
		BucketItem b = this.bucket[idx];
		b.height++;
		b.part = b.height * 1.0 / ntups;

	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this
	 * table.
	 * 
	 * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate
	 * of the fraction of elements that are greater than 5.
	 * 
	 * @param op
	 *            Operator
	 * @param v
	 *            Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	// EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ,
	// LIKE, NOT_EQUALS;
	public double estimateSelectivity(Predicate.Op op, int v) {
		int idx = calculateIdx(v);

		double selectivity = 0;

		switch (op) {
		case EQUALS: {
			if (v < this.min || v > this.max)
				return 0;

			/*
			 * if range is 0.1, then the height/range is not correct
			 */
			// double ret = (this.bucket[idx].height / width) / ntups;
			return (this.bucket[idx].height / width) / ntups;
		}
		case GREATER_THAN: {
			if (v < this.min)
				return 1;

			if (v >= this.max)
				return 0;

			double b_part = bucket[idx].getRightPart(v);

			for (int i = idx + 1; i < bucket.length; i++) {
				selectivity += this.bucket[i].height;
			}
			selectivity = selectivity / ntups;
			return selectivity + b_part;
		}
		case LESS_THAN: {
			if (v <= this.min)
				return 0;

			if (v > this.max)
				return 1;
			double b_part = bucket[idx].getLeftPart(v);
			for (int i = 0; i < idx; i++) {
				selectivity += this.bucket[i].height;
			}
			selectivity = selectivity / ntups;
			return selectivity + b_part;

		}
		case LESS_THAN_OR_EQ: {
			if (v < this.min)
				return 0;
			if (v >= this.max)
				return 1;

			if (idx > bucket.length)
				return 1;

			for (int i = 0; i < idx; i++)
				selectivity += this.bucket[i].part;
			return selectivity + (this.bucket[idx].height / width) / ntups;
		}
		case GREATER_THAN_OR_EQ: {
			if (v <= this.min)
				return 1;
			if (v > this.max)
				return 0;

			for (int i = idx + 1; i < bucket.length; i++)
				selectivity += this.bucket[i].part;

			return selectivity + (this.bucket[idx].height / width) / ntups;
		}
		case LIKE:
			new util.Todo();
			break;
		case NOT_EQUALS: {
			if (v < this.min || v > this.max)
				return 1;

			return 1 - ((this.bucket[idx].height / width) / ntups);
		}

		default:

			new util.Bug("undefine operation");

		}

		return -1.0;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {

		StringBuilder sb = new StringBuilder();
		for (BucketItem b : this.bucket) {
			sb.append(b.left);
			sb.append("~");
			sb.append(b.right);
			sb.append(": ");
			sb.append(b.height);
			sb.append("\n");

		}

		return sb.toString();
	}
}

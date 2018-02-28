package sim.util;

import java.util.Arrays;

public class DoublePoint implements Comparable<DoublePoint> {
	public int nd;
	public double[] c;

	public DoublePoint(double c[]) {
		this.nd = c.length;
		this.c = Arrays.copyOf(c, nd);
	}

	public DoublePoint(double c1) {
		this.nd = 1;
		this.c = new double[]{c1};
	}

	public DoublePoint(double c1, double c2) {
		this.nd = 2;
		this.c = new double[]{c1, c2};
	}

	public DoublePoint(double c1, double c2, double c3) {
		this.nd = 3;
		this.c = new double[]{c1, c2, c3};
	}

	public double rectSize(DoublePoint that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		if (nd == 0)
			return 0;

		double ret = this.c[0] - that.c[0];
		for(int i = 1; i < nd; i++)
			ret *= this.c[i] - that.c[i];

		return Math.abs(ret);
	}

	public void shift(int dim, double offset) {
		if (dim < 0 || dim >= nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);
		c[dim] += offset;
	}

	public void shift(double[] offsets) {
		if (nd != offsets.length)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + offsets.length);

		for (int i = 0; i < nd; i++)
			shift(i, offsets[i]);
	}

	// Reduce dimension by removing the value at the dimth dimension
	public DoublePoint reduceDim(int dim) {
		if (dim < 0 || dim >= nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);
		
		double[] newc = Arrays.copyOf(c, nd - 1);
		for(int i = dim; i < nd - 1; i++)
			newc[i] = c[i + 1];

		return new DoublePoint(newc);
	}

	// Increase the dimension by inserting the val into the dimth dimension
	public DoublePoint increaseDim(int dim, double val) {
		if (dim < 0 || dim > nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);

		double[] newc = Arrays.copyOf(c, nd + 1);
		for(int i = dim; i < nd; i++)
			newc[i + 1] = c[i];
		newc[dim] = val;

		return new DoublePoint(newc);
	}

	// Sort the points by their components
	@Override
	public int compareTo(DoublePoint that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		for(int i = 0; i < nd; i++) {
			if (this.c[i] == that.c[i])
				continue;
			if (this.c[i] > that.c[i])
				return 1;
			if (this.c[i] < that.c[i])
				return -1;
		}

		return 0;
	}

	public String toString() {
		return Arrays.toString(c);
	}
}
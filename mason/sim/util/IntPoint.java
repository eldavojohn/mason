package sim.util;

import java.util.Arrays;

public class IntPoint implements Comparable<IntPoint> {
	public int nd;
	public int[] c;

	public IntPoint(int c[]) {
		this.nd = c.length;
		this.c = Arrays.copyOf(c, nd);
	}

	public IntPoint(int c1) {
		this.nd = 1;
		this.c = new int[]{c1};
	}

	public IntPoint(int c1, int c2) {
		this.nd = 2;
		this.c = new int[]{c1, c2};
	}

	public IntPoint(int c1, int c2, int c3) {
		this.nd = 3;
		this.c = new int[]{c1, c2, c3};
	}

	public int rectSize(IntPoint that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		if (nd == 0)
			return 0;

		int ret = this.c[0] - that.c[0];
		for(int i = 1; i < nd; i++)
			ret *= this.c[i] - that.c[i];

		return Math.abs(ret);
	}

	public void shift(int dim, int offset) {
		if (dim < 0 || dim >= nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);
		c[dim] += offset;
	}

	public void shift(int[] offsets) {
		if (nd != offsets.length)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + offsets.length);

		for (int i = 0; i < nd; i++)
			shift(i, offsets[i]);
	}

	// Reduce dimension by removing the value at the dimth dimension
	public IntPoint reduceDim(int dim) {
		if (dim < 0 || dim >= nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);
		
		int[] newc = Arrays.copyOf(c, nd - 1);
		for(int i = dim; i < nd - 1; i++)
			newc[i] = c[i + 1];

		return new IntPoint(newc);
	}

	// Increase the dimension by inserting the val into the dimth dimension
	public IntPoint increaseDim(int dim, int val) {
		if (dim < 0 || dim > nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);

		int[] newc = Arrays.copyOf(c, nd + 1);
		for(int i = dim; i < nd; i++)
			newc[i + 1] = c[i];
		newc[dim] = val;

		return new IntPoint(newc);
	}

	@Override
	public int compareTo(IntPoint that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		for(int i = 0; i < nd; i++) {
			if (this.c[i] == that.c[i])
				continue;
			return this.c[i] - that.c[i];
		}

		return 0;
	}
}
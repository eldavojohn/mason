package sim.util;

import java.util.Arrays;
import java.util.stream.IntStream;

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

	public int getRectArea(IntPoint that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		if (nd == 0)
			return 0;

		return Math.abs(Arrays.stream(getOffset(that)).reduce(1, (x, y) -> x * y));
	}

	public IntPoint shift(int dim, int offset) {
		if (dim < 0 || dim >= nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);
		
		int[] newc = Arrays.copyOf(c, nd);
		newc[dim] += offset;
		
		return new IntPoint(newc);
	}

	public IntPoint shift(int[] offsets) {
		if (nd != offsets.length)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + offsets.length);

		int[] newc = Arrays.copyOf(c, nd);

		for (int i = 0; i < nd; i++)
			newc[i] += offsets[i];

		return new IntPoint(newc);
	}

	public int[] getOffset(IntPoint that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		return IntStream.range(0, nd).map(i -> this.c[i] - that.c[i]).toArray();
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

	// Sort the points by their components
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

	public String toString() {
		return Arrays.toString(c);
	}
}
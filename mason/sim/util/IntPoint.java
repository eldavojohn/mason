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

	// Sanity checks
	public void assertEqualDim(int d) {
		if (d < 0 || d >= this.nd)
			throw new IllegalArgumentException(String.format("Illegal dimension %d given to %d", d, this.toString()));
	}

	public void assertEqualDim(int[] a) {
		if (this.nd != a.length)
			throw new IllegalArgumentException(String.format("%s and %s got different dimensions", this.toString(), Arrays.toString(a)));
	}

	public void assertEqualDim(IntPoint p) {
		if (this.nd != p.nd)
			throw new IllegalArgumentException(String.format("%s and %s got different dimensions", this.toString(), p.toString()));
	}

	public int getRectArea(IntPoint that) {
		assertEqualDim(that);
		return nd == 0 ? 0 : Math.abs(Arrays.stream(getOffset(that)).reduce(1, (x, y) -> x * y));
	}

	public IntPoint shift(int dim, int offset) {
		assertEqualDim(dim);
		
		int[] newc = Arrays.copyOf(c, nd);
		newc[dim] += offset;
		
		return new IntPoint(newc);
	}

	public IntPoint shift(int[] offsets) {
		assertEqualDim(offsets);
		return new IntPoint(IntStream.range(0, nd).map(i -> c[i] + offsets[i]).toArray());
	}

	public IntPoint rshift(int[] offsets) {
		assertEqualDim(offsets);
		return new IntPoint(IntStream.range(0, nd).map(i -> c[i] - offsets[i]).toArray());
	}

	// Get the distances in each dimension between self and the given point
	public int[] getOffset(IntPoint that) {
		assertEqualDim(that);
		return IntStream.range(0, nd).map(i -> this.c[i] - that.c[i]).toArray();
	}

	// Reduce dimension by removing the value at the dimth dimension
	public IntPoint reduceDim(int dim) {
		assertEqualDim(dim);
		
		int[] newc = Arrays.copyOf(c, nd - 1);
		for(int i = dim; i < nd - 1; i++)
			newc[i] = c[i + 1];

		return new IntPoint(newc);
	}

	// Return whether the two IntPoints equals (have same value for all of their components)
	public boolean equals(IntPoint that) {
		assertEqualDim(that);

		for (int i = 0; i < nd; i++)
			if (this.c[i] != that.c[i])
				return false;

		return true;
	}

	// // Increase the dimension by inserting the val into the dimth dimension
	// public IntPoint increaseDim(int dim, int val) {
	// 	if (dim < 0 || dim > nd)
	// 		throw new IllegalArgumentException("Illegal dimension: " + dim);

	// 	int[] newc = Arrays.copyOf(c, nd + 1);
	// 	for(int i = dim; i < nd; i++)
	// 		newc[i + 1] = c[i];
	// 	newc[dim] = val;

	// 	return new IntPoint(newc);
	// }

	// Sort the points by their components
	@Override
	public int compareTo(IntPoint that) {
		assertEqualDim(that);

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
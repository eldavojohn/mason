package sim.util;

import java.util.Arrays;
import java.util.stream.IntStream;

public class IntHyperRect implements Comparable<IntHyperRect> {
	public int nd, id;
	public IntPoint ul, br;

	public IntHyperRect(int id, IntPoint ul, IntPoint br) {
		this.id = id;

		if (ul.nd != br.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + ul.nd + " and " + br.nd);

		this.nd = ul.nd;

		for (int i = 0; i < nd; i++)
			if (br.c[i] < ul.c[i])
				throw new IllegalArgumentException("All p2's components should be greater than or equal to p1's corresponding one");

		this.ul = ul;
		this.br = br;
	}

	// Return the area of the hyper rectangle
	public int getArea() {
		return br.getRectArea(ul);
	}

	// Return the size of the hyper rectangle in each dimension
	public int[] getSize() {
		return br.getOffset(ul);
	}

	// Return whether the rect contains p
	// Noted that the rect is treated as half-inclusive (ul) and half-exclusive (br)
	public boolean contains(IntPoint p) {
		ul.assertEqualDim(p);
		return IntStream.range(0, p.nd).allMatch(i -> ul.c[i] <= p.c[i] && p.c[i] < br.c[i]);
	}

	// Return whether the given rect intersects with self
	public boolean isIntersect(IntHyperRect that) {
		ul.assertEqualDim(that.ul);
		return IntStream.range(0, nd).allMatch(i -> this.ul.c[i] < that.br.c[i] && this.br.c[i] > that.ul.c[i]);
	}

	// Return the intersection of the given rect and self
	public IntHyperRect getIntersection(IntHyperRect that) {
		if (!isIntersect(that))
			throw new IllegalArgumentException(this + " does not intersect with " + that);

		int[] c1 = IntStream.range(0, nd).map(i -> Math.max(this.ul.c[i], that.ul.c[i])).toArray();
		int[] c2 = IntStream.range(0, nd).map(i -> Math.min(this.br.c[i], that.br.c[i])).toArray();

		return new IntHyperRect(-1, new IntPoint(c1), new IntPoint(c2));
	}

	// Symmetric resize 
	public IntHyperRect resize(int dim, int val) {
		return new IntHyperRect(id, ul.shift(dim, -val), br.shift(dim, val));
	}

	// Symmetric resize at all dimension
	public IntHyperRect resize(int[] vals) {
		return new IntHyperRect(id, ul.rshift(vals), br.shift(vals));
	}

	// One-sided resize
	public IntHyperRect resize(int dim, int dir, int val) {
		if (dir > 0)
			return new IntHyperRect(id, ul.shift(dim, 0), br.shift(dim, val));
		return new IntHyperRect(id, ul.shift(dim, -val), br.shift(dim, 0));
	}

	// One-sided resize at all dimension
	public IntHyperRect resize(int dir, int[] vals) {
		if (dir > 0)
			return new IntHyperRect(id, ul.shift(0, 0), br.shift(vals));
		return new IntHyperRect(id, ul.rshift(vals), br.shift(0, 0));
	}

	// Move the rect by offset in the dimth dimension
	public IntHyperRect shift(int dim, int offset) {
		return new IntHyperRect(id, ul.shift(dim, offset), br.shift(dim, offset));
	}

	// Move the rect by the given offsets
	public IntHyperRect shift(int[] offsets) {
		return new IntHyperRect(id, ul.shift(offsets), br.shift(offsets));
	}

	// Return the segment of the hyper rectangle on the given dimension
	public Segment getSegment(int dim) {
		ul.assertEqualDim(dim);
		return new Segment((double)ul.c[dim], (double)br.c[dim], id);
	}

	// Sort the rectangles based on its upper left corner first and then bottom-right corner and then id
	@Override
	public int compareTo(IntHyperRect that) {
		int ret;

		if ((ret = this.ul.compareTo(that.ul)) != 0)
			return ret;
		if ((ret = this.br.compareTo(that.br)) != 0)
			return ret;

		return this.id - that.id;
	}

	public String toString() {
		return String.format("%s<%d, %s, %s>", this.getClass().getSimpleName(), id, ul.toString(), br.toString());
	}

	// Return a copy of the hyper rectangle with the given dimension removed
	public IntHyperRect reduceDim(int dim) {
		return new IntHyperRect(id, ul.reduceDim(dim), br.reduceDim(dim));
	}

	// public int[] getRelPos(IntHyperRect that) {
	// 	assertEqualDim(that);

	// 	return IntStream.range(0, this.nd)
	// 	       .map(i -> this.ul.c[i] >= that.br.c[i] ? -1 : (that.ul.c[i] >= this.br.c[i]) ? 1 : 0)
	// 	       .toArray();
	// }

	public static void main(String[] args) {
		IntPoint p1 = new IntPoint(new int[] {1, 1});
		IntPoint p2 = new IntPoint(new int[] {0, 3});
		IntPoint p3 = new IntPoint(new int[] {4, 4});
		IntPoint p4 = new IntPoint(new int[] {2, 6});
		IntHyperRect r1 = new IntHyperRect(0, p1, p3);
		IntHyperRect r2 = new IntHyperRect(1, p2, p4);

		System.out.println(r1.getIntersection(r2));

		IntPoint p5 = new IntPoint(new int[] {0, 4});
		IntHyperRect r3 = new IntHyperRect(1, p5, p4);

		System.out.println(r1.isIntersect(r3));
	}
}
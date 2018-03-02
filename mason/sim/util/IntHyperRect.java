package sim.util;

import java.util.Arrays;
import java.util.stream.IntStream;

public class IntHyperRect implements Comparable<IntHyperRect> {
	public int nd, id;
	public IntPoint ul, br;

	public IntHyperRect(int id, IntPoint ul, IntPoint br) {
		if (ul.nd != br.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + ul.nd + " and " + br.nd);

		this.nd = ul.nd;

		for (int i = 0; i < nd; i++)
			if (br.c[i] < ul.c[i])
				throw new IllegalArgumentException("All p2's components should be greater than or equal to p1's corresponding one");

		this.ul = ul;
		this.br = br;
		this.id = id;
	}

	public boolean contains(IntPoint p) {
		if (this.nd != p.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + p.nd);

		return IntStream.range(0, p.nd).allMatch(i -> ul.c[i] <= p.c[i] && p.c[i] < br.c[i]);
	}

	public boolean isIntersect(IntHyperRect that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		return IntStream.range(0, nd).allMatch(i -> this.ul.c[i] < that.br.c[i] && this.br.c[i] > that.ul.c[i]);
	}

	public int[] getRelPos(IntHyperRect that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		return IntStream.range(0, this.nd)
		       .map(i -> this.ul.c[i] >= that.br.c[i] ? -1 : (that.ul.c[i] >= this.br.c[i]) ? 1 : 0)
		       .toArray();
	}

	public IntHyperRect intersect(IntHyperRect that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		if (!isIntersect(that))
			throw new IllegalArgumentException("Given " + that + " does not intersect with this " + this);

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
		int[] nvals = Arrays.stream(vals).map(x -> -x).toArray();
		return new IntHyperRect(id, ul.shift(nvals), br.shift(vals));
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
		int[] nvals = Arrays.stream(vals).map(x -> -x).toArray();
		return new IntHyperRect(id, ul.shift(nvals), br.shift(0, 0));
	}

	public Segment getSegment(int dim) {
		if (dim < 0 || dim >= nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);
		return new Segment((double)ul.c[dim], (double)br.c[dim], id);
	}

	public int getArea() {
		return br.getRectArea(ul);
	}

	public int[] getSize() {
		return br.getOffset(ul);
	}

	public IntHyperRect reduceDim(int dim) {
		return new IntHyperRect(id, ul.reduceDim(dim), br.reduceDim(dim));
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

	public static void main(String[] args) {
		IntPoint p1 = new IntPoint(new int[] {1, 1});
		IntPoint p2 = new IntPoint(new int[] {0, 3});
		IntPoint p3 = new IntPoint(new int[] {4, 4});
		IntPoint p4 = new IntPoint(new int[] {2, 6});
		IntHyperRect r1 = new IntHyperRect(0, p1, p3);
		IntHyperRect r2 = new IntHyperRect(1, p2, p4);

		System.out.println(r1.intersect(r2));

		IntPoint p5 = new IntPoint(new int[] {0, 4});
		IntHyperRect r3 = new IntHyperRect(1, p5, p4);

		System.out.println(r1.isIntersect(r3));
	}
}
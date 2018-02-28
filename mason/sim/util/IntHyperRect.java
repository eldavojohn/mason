package sim.util;

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

	public boolean isOverlap(IntHyperRect that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);
		
		return this.contains(that.ul) || this.contains(that.br) || that.contains(this.ul) || that.contains(this.br);
	}

	public int[] getRelPos(IntHyperRect that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);

		return IntStream.range(0, this.nd)
		       .map(i -> this.ul.c[i] >= that.br.c[i] ? -1 : (that.ul.c[i] >= this.br.c[i]) ? 1 : 0)
		       .toArray();
	}

	public Segment getSegment(int dim) {
		if (dim < 0 || dim >= nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);
		return new Segment((double)ul.c[dim], (double)br.c[dim], id);
	}

	public int getSize() {
		return br.rectSize(ul);
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
}
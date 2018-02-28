package sim.util;

public class IntHyperRect implements Comparable<IntHyperRect> {
	int nd;
	IntPoint ul, br;

	public IntHyperRect(IntPoint ul, IntPoint br) {
		if (ul.nd != br.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + ul.nd + " and " + br.nd);

		this.nd = ul.nd;

		for (int i = 0; i < nd; i++)
			if (br.c[i] < ul.c[i])
				throw new IllegalArgumentException("All p2's components should be greater than or equal to p1's corresponding one");

		this.ul = ul;
		this.br = br;
	}

	public boolean isOverlap(IntHyperRect that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);
		//TODO
		return false;
	}

	public int[] getRelPos(IntHyperRect that) {
		if (this.nd != that.nd)
			throw new IllegalArgumentException("Number of dimensions must be the same. Got " + this.nd + " and " + that.nd);
		//TODO
		return new int[] {};
	}

	public Segment getSegment(int dim) {
		if (dim < 0 || dim >= nd)
			throw new IllegalArgumentException("Illegal dimension: " + dim);
		return new Segment((double)ul.c[dim], (double)br.c[dim]);
	}

	public int getSize() {
		return br.rectSize(ul);
	}

	public IntHyperRect reduceDim(int dim) {
		return new IntHyperRect(ul.reduceDim(dim), br.reduceDim(dim));
	}

	// Sort the rectangles based on its upper left corner first and then bottom-right corner
	@Override
	public int compareTo(IntHyperRect that) {
		int ret = this.ul.compareTo(that.ul);
		if (ret != 0)
			return ret;
		return this.br.compareTo(that.br);
	}
}
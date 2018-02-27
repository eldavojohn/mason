package sim.field;

import java.util.Arrays;

import mpi.*;

public class HaloFieldGrid {

	int aoi, nd;
	int[] lb, ub, lsize, gsize;

	public HaloFieldGrid(DUniformPartition p, int aoi) {
		this.nd = p.dims.length;
		this.aoi = aoi;

		gsize = Arrays.copyOf(p.size, nd);
		lsize = new int[nd];
		lb = new int[nd];
		ub = new int[nd];
		for (int i = 0; i < nd; i++) {
			lsize[i] = p.size[i] / p.dims[i];
			lb[i] = lsize[i] * p.coords[i];
			ub[i] = lb[i] + lsize[i];
		}
	}

	public HaloFieldGrid(DNonUniformPartition p, int aoi) {
		this.nd = p.nd;
		this.aoi = aoi;

		Partition myPart = p.getMyPartition();
		gsize = Arrays.copyOf(p.size, nd);
		// lb = Arrays.copyOf(myPart.ul, nd);
		// ub = Arrays.copyOf(myPart.br, nd);
		// TODO: fix the type mismatch here
		lb = Arrays.stream(myPart.ul).mapToInt(x -> (int)x).toArray();
		ub = Arrays.stream(myPart.br).mapToInt(x -> (int)x).toArray();
		lsize = new int[nd];
		for (int i = 0; i < nd; i++) 
			lsize[i] = ub[i] - lb[i];
	}

	public boolean inGlobal(int[] c) {
		boolean ret = true;
		for (int i = 0; i < c.length; i++)
			ret &= c[i] >= 0 && c[i] < gsize[i];
		return ret;
	}

	public boolean inLocal(int[] c) {
		boolean ret = true;
		for (int i = 0; i < c.length; i++)
			ret &= c[i] >= lb[i] && c[i] < ub[i];
		return ret;
	}

	public boolean inPrivate(int[] c) {
		boolean ret = true;
		for (int i = 0; i < c.length; i++)
			ret &= c[i] >= lb[i] + aoi && c[i] < ub[i] - aoi;
		return ret;
	}

	public boolean inShared(int[] c) {
		return inLocal(c) && !inPrivate(c);
	}

	public boolean inHalo(int[] c) {
		return inLocalAndHalo(c) && !inLocal(c);
	}

	public boolean inLocalAndHalo(int[] c) {
		boolean ret = true;
		for (int i = 0; i < c.length; i++) {
			int lc = toLocalCoord(c[i], i);
			ret &= lc >= 0 && lc < lsize[i] + 2 * aoi;
		}
		return ret;
	}

	public int[] toToroidal(int[] c) {
		for (int i = 0; i < c.length; i++) {
			if (c[i] < 0)
				c[i] += gsize[i];
			else if (c[i] >= gsize[i])
				c[i] -= gsize[i];
		}
		return c;
	}

	public int[] toLocalCoords(int[] c) {
		for (int i = 0; i < c.length; i++)
			c[i] = toLocalCoord(c[i], i);
		return c;
	}

	private int toLocalCoord(int c, int i) {
		int lc = c - lb[i] + aoi;
		if (lc < 0)
			lc += gsize[i];
		else if (lc >= lsize[i] + 2 * aoi)
			lc -= gsize[i];
		return lc;
	}

	public static void main(String args[]) throws MPIException {
		int[] size = new int[] {100, 200};
		int aoi = 10;
		int[] want, got;

		MPI.Init(args);

		DUniformPartition p = new DUniformPartition(size);
		//DNonUniformPartition p = new DNonUniformPartition(size);
		//p.initUniformly();
		HaloFieldGrid hf = new HaloFieldGrid(p, aoi);

		assert p.np == 4;

		assert hf.inGlobal(new int[] {59, 82});
		assert !hf.inGlobal(new int[] { -3, 40});
		assert !hf.inGlobal(new int[] {35, 240});

		want = new int[] {70, 50};
		got = hf.toToroidal(new int[] { -30, 250});
		assert Arrays.equals(want, got);

		want = new int[] {20, 180};
		got = hf.toToroidal(new int[] {120, -20});
		assert Arrays.equals(want, got);

		if (p.pid == 0) {
			assert hf.inLocal(new int[] {0, 99});
			assert !hf.inLocal(new int[] {50, 0});

			assert hf.inPrivate(new int[] {aoi, aoi});
			assert !hf.inPrivate(new int[] {50, 0});
			assert !hf.inPrivate(new int[] {0, 99});

			assert hf.inShared(new int[] {49, 99});
			assert !hf.inShared(new int[] {50, 100});
			assert !hf.inShared(new int[] {25, 50});

			assert hf.inHalo(new int[] {0, 100});
			assert hf.inHalo(new int[] {0, 199});
			assert hf.inHalo(new int[] {95, 100});
			assert !hf.inHalo(new int[] {25, 50});
			assert !hf.inHalo(new int[] {75, 50});

			want = new int[] {30, 60};
			got = hf.toLocalCoords(new int[] {20, 50});
			assert Arrays.equals(want, got);

			want = new int[] {5, 5};
			got = hf.toLocalCoords(new int[] {95, 195});
			assert Arrays.equals(want, got);
		}

		MPI.Finalize();
	}
}
package sim.field.grid;

import java.io.IOException;

import mpi.*;

import sim.util.IntPoint;
import sim.util.IntHyperRect;
import sim.util.MPITest;
import sim.field.DPartition;
import sim.field.DNonUniformPartition;
import sim.field.HaloField;
import sim.field.storage.IntGridStorage;

public class NIntGrid2D extends HaloField {

	public NIntGrid2D(DPartition ps, int[] aoi, int initVal) {
		super(ps, aoi, new IntGridStorage(ps.getPartition(), initVal));

		if (this.nd != 2)
			throw new IllegalArgumentException("The number of dimensions is expected to be 2, got: " + this.nd);
	}

	public int[] getStorageArray() {
		return (int[])field.getStorage();
	}

	public final int get(final int x, final int y) {
		return get(new IntPoint(x, y));
	}

	public final int get(IntPoint p) {
		// In this partition and its surrounding ghost cells
		if (!inLocalAndHalo(p))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of local boundary", ps.getPid(), p.toString()));

		return getStorageArray()[field.getFlatIdx(toLocalPoint(p))];
	}

	public final void set(final int x, final int y, final int val) {
		set(new IntPoint(x, y), val);
	}

	public final void set(IntPoint p, final int val) {
		// In this partition but not in ghost cells
		if (!inLocal(p))
			throw new IllegalArgumentException(String.format("PID %d set %s is out of local boundary", ps.getPid(), p.toString()));

		getStorageArray()[field.getFlatIdx(toLocalPoint(p))] = val;
	}

	public static void main(String[] args) throws MPIException, IOException {
		MPI.Init(args);

		int[] aoi = new int[] {2, 2};
		int[] size = new int[] {8, 8};

		DNonUniformPartition p = DNonUniformPartition.getPartitionScheme(size, true);
		p.initUniformly(null);
		p.commit();

		NIntGrid2D f = new NIntGrid2D(p, aoi, p.getPid());

		f.sync();

		MPITest.execInOrder(i -> System.out.println(f), 500);

		MPI.Finalize();
	}
}

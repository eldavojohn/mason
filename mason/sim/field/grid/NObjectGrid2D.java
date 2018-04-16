package sim.field.grid;

import java.io.IOException;
import java.util.function.IntFunction;

import mpi.*;

import sim.util.IntPoint;
import sim.util.IntHyperRect;
import sim.util.MPITest;
import sim.field.DPartition;
import sim.field.DNonUniformPartition;
import sim.field.HaloField;
import sim.field.storage.GridStorage;
import sim.field.storage.ObjectGridStorage;
import sim.field.storage.TestObj;

public class NObjectGrid2D<T> extends HaloField {

	public NObjectGrid2D(DPartition ps, int[] aoi, IntFunction<T[]> allocator) {
		super(ps, aoi, new ObjectGridStorage<T>(ps.getPartition(), allocator));

		if (this.nd != 2)
			throw new IllegalArgumentException("The number of dimensions is expected to be 2, got: " + this.nd);
	}

	public T[] getStorageArray() {
		return (T[])field.getStorage();
	}

	public final T get(final int x, final int y) {
		return get(new IntPoint(x, y));
	}

	public final T get(IntPoint p) {
		// In this partition and its surrounding ghost cells
		if (!inLocalAndHalo(p))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of local boundary", ps.getPid(), p.toString()));

		return getStorageArray()[field.getFlatIdx(toLocalPoint(p))];
	}

	public final void set(final int x, final int y, final T val) {
		set(new IntPoint(x, y), val);
	}

	public final void set(IntPoint p, final T val) {
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

		NObjectGrid2D<TestObj> f = new NObjectGrid2D<TestObj>(p, aoi, s -> new TestObj[s]);

		MPITest.execOnlyIn(0, x -> {
			f.set(0, 3, new TestObj(0));
			f.set(1, 2, new TestObj(1));
			f.set(2, 1, new TestObj(2));
			f.set(3, 0, new TestObj(3));
		});

		MPITest.execOnlyIn(2, x -> {
			f.set(4, 0, new TestObj(4));
			f.set(5, 1, new TestObj(5));
			f.set(6, 2, new TestObj(6));
			f.set(7, 3, new TestObj(7));
		});

		MPITest.execOnlyIn(0, i -> System.out.println("Before Sync..."));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		f.sync();

		MPITest.execOnlyIn(0, i -> System.out.println("After Sync..."));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		GridStorage full = new ObjectGridStorage<TestObj>(p.getField(), s -> new TestObj[s]);
		f.collect(0, full);

		MPITest.execOnlyIn(0, i -> System.out.println("Full Field...\n" + full));

		MPI.Finalize();
	}
}

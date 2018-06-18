package sim.field.continuous;

import java.io.Serializable;
import java.util.List;
import java.rmi.Remote;
import java.rmi.RemoteException;

import mpi.*;

import sim.field.DPartition;
import sim.field.DNonUniformPartition;
import sim.field.HaloField;
import sim.field.storage.ContStorage;
import sim.field.storage.TestObj;
import sim.util.DoublePoint;
import sim.util.IntPoint;
import sim.util.NdPoint;
import sim.util.MPIUtil;
import sim.util.MPITest;

public class NContinuous2D<T extends Serializable> extends HaloField {

	public NContinuous2D(DPartition ps, int[] aoi, int[] discretizations) {
		super(ps, aoi, new ContStorage<T>(ps.getPartition(), discretizations));

		if (this.nd != 2)
			throw new IllegalArgumentException("The number of dimensions is expected to be 2, got: " + this.nd);
	}

	public final NdPoint getLocation(T obj) {
		return ((ContStorage)field).getLocation(obj);
	}

	public final void setLocation(T obj, NdPoint p) {
		((ContStorage)field).setLocation(obj, p);
	}

	public final List<T> getObjects(NdPoint p) {
		return ((ContStorage)field).getObjects(p);
	}

	public final List<T> getNearestNeighbors(T obj, int k) {
		return ((ContStorage)field).getNearestNeighbors(obj, k);
	}

	public final List<T> getNeighborsWithin(T obj, double r) {
		return ((ContStorage)field).getNeighborsWithin(obj, r);
	}

	public final void removeObject(T obj) {
		((ContStorage)field).removeObject(obj);
	}

	public final void removeObjects(NdPoint p) {
		((ContStorage)field).removeObjects(p);
	}

	// TODO
	public Serializable getRMI(IntPoint p) throws RemoteException {
		//return ((ContStorage)field).getObjects(obj);
		return null;
	}

	public static void main(String[] args) throws MPIException {
		MPI.Init(args);

		int[] aoi = new int[] {10, 10};
		int[] size = new int[] {1000, 1000};
		int[] discretizations = new int[] {10, 10};

		DNonUniformPartition p = DNonUniformPartition.getPartitionScheme(size, true, aoi);
		p.initUniformly(null);
		p.commit();

		NContinuous2D<TestObj> f = new NContinuous2D<TestObj>(p, aoi, discretizations);

		MPITest.execOnlyIn(0, x -> {
			f.setLocation(new TestObj(0), new DoublePoint(5.5, 5.5));
			f.setLocation(new TestObj(1), new DoublePoint(10.1, 10.1));
			f.setLocation(new TestObj(2), new DoublePoint(5.5, 250));
			f.setLocation(new TestObj(3), new DoublePoint(495, 495));
		});

		MPITest.execOnlyIn(2, x -> {
			f.setLocation(new TestObj(4), new DoublePoint(500, 250));
			f.setLocation(new TestObj(5), new DoublePoint(750, 495));
			f.setLocation(new TestObj(6), new DoublePoint(750, 250));
			f.setLocation(new TestObj(7), new DoublePoint(751, 495));
		});

		MPITest.execOnlyIn(0, i -> System.out.println("Before Sync..."));
		MPITest.execInOrder(i -> System.out.println(f), 200);

		f.sync();

		MPITest.execOnlyIn(0, i -> System.out.println("After Sync..."));
		MPITest.execInOrder(i -> System.out.println(f), 200);

		ContStorage full = new ContStorage<TestObj>(p.getField(), discretizations);
		f.collect(0, full);

		MPITest.execOnlyIn(0, i -> System.out.println("Full Field...\n" + full));

		MPI.Finalize();
	}
}
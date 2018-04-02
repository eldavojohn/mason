package sim.field.storage;

import java.util.Arrays;

import mpi.*;
import static mpi.MPI.slice;

import sim.util.IntHyperRect;
import sim.util.MPIParam;

public class DoubleGridStorage extends GridStorage {

	public DoubleGridStorage(IntHyperRect shape, double initVal) {
		super(shape, initVal);
		baseType = MPI.DOUBLE;
	}

	public int pack(MPIParam mp, byte[] buf, int idx) throws MPIException {
		return MPI.COMM_WORLD.pack(slice((double[])storage, mp.idx), 1, mp.type, buf, idx);
	}

	public int unpack(MPIParam mp, byte[] buf, int idx, int len) throws MPIException {
		return MPI.COMM_WORLD.unpack(buf, idx, slice((double[])storage, mp.idx), 1, mp.type);
	}

	protected Object allocate(int size, Object initVal) {
		double[] array = new double[size];
		Arrays.fill(array, (double)initVal);
		return array;
	}
}

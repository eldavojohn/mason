package sim.field.storage;

import java.util.Arrays;

import mpi.*;
import static mpi.MPI.slice;

import sim.util.IntHyperRect;
import sim.util.MPIParam;

public class IntGridStorage extends GridStorage {

	public IntGridStorage(IntHyperRect shape, int initVal) {
		super(shape, initVal);
		baseType = MPI.INT;
	}

	public int pack(MPIParam mp, byte[] buf, int idx) throws MPIException {
		return MPI.COMM_WORLD.pack(slice((int[])storage, mp.idx), 1, mp.type, buf, idx);
	}

	public int unpack(MPIParam mp, byte[] buf, int idx, int len) throws MPIException {
		return MPI.COMM_WORLD.unpack(buf, idx, slice((int[])storage, mp.idx), 1, mp.type);
	}

	protected Object allocate(int size, Object initVal) {
		int[] array = new int[size];
		Arrays.fill(array, (int)initVal);
		return array;
	}
}
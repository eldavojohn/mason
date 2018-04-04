package sim.field.storage;

import java.util.Arrays;

import mpi.*;
import static mpi.MPI.slice;

import sim.util.IntHyperRect;
import sim.util.MPIParam;

public class DoubleGridStorage extends GridStorage {

	public DoubleGridStorage(IntHyperRect shape, double initVal) {
		super(shape);
		baseType = MPI.DOUBLE;
		storage = allocate(shape.getArea());
		Arrays.fill((double[])storage, initVal);
	}

	public int pack(MPIParam mp, byte[] buf, int idx) throws MPIException {
		return MPI.COMM_WORLD.pack(slice((double[])storage, mp.idx), 1, mp.type, buf, idx);
	}

	public int unpack(MPIParam mp, byte[] buf, int idx, int len) throws MPIException {
		return MPI.COMM_WORLD.unpack(buf, idx, slice((double[])storage, mp.idx), 1, mp.type);
	}

	public String toString() {
		int[] size = shape.getSize();
		double[] array = (double[])storage;
		StringBuffer buf = new StringBuffer(String.format("DoubleGridStorage-%s\n", shape));

		if (shape.nd == 2)
			for (int i = 0; i < size[0]; i++) {
				for (int j = 0; j < size[1]; j++)
					buf.append(String.format(" %4.2f ", array[i * size[1] + j]));
				buf.append("\n");
			}

		return buf.toString();
	}

	protected Object allocate(int size) {
		return new double[size];
	}
}

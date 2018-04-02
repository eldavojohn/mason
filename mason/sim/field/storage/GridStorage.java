package sim.field.storage;

import java.util.stream.IntStream;

import mpi.*;

import sim.field.HaloField;
import sim.util.IntHyperRect;
import sim.util.IntPoint;
import sim.util.MPIParam;

public abstract class GridStorage {
	Object storage, initVal;
	IntHyperRect shape;
	Datatype baseType;

	int[] stride;

	public GridStorage(IntHyperRect shape, Object initVal) {
		this.shape = shape;
		this.initVal = initVal;
		this.storage = allocate(shape.getArea(), initVal);
		this.stride = getStride(shape.getSize());
	}

	public Object getStorage() {
		return storage;
	}

	public Datatype getMPIBaseType() {
		return baseType;
	}

	public abstract int pack(MPIParam mp, byte[] buf, int idx) throws MPIException;
	public abstract int unpack(MPIParam mp, byte[] buf, int idx, int len) throws MPIException;
	protected abstract Object allocate(int size, Object initVal);

	public void reshape(IntHyperRect newShape) {
		if (newShape.isIntersect(shape)) {
			IntHyperRect overlap = newShape.getIntersection(shape);
			MPIParam fromParam = new MPIParam(overlap, shape, baseType);
			MPIParam toParam = new MPIParam(overlap, newShape, baseType);

			try {
				byte[] buf = new byte[MPI.COMM_WORLD.packSize(overlap.getArea(), baseType)];
				pack(fromParam, buf, 0);

				storage = allocate(newShape.getArea(), initVal);
				unpack(toParam, buf, 0, 0);

				fromParam.type.free();
				toParam.type.free();
			} catch (MPIException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		} else
			storage = allocate(newShape.getArea(), initVal);

		shape = newShape;
		stride = getStride(shape.getSize());
	}

	public int getFlatIdx(IntPoint p) {
		return IntStream.range(0, p.nd).map(i -> p.c[i] * stride[i]).sum();
	}

	// Get the flatted index with respect to the given size
	public static int getFlatIdx(IntPoint p, int[] wrtSize) {
		int[] s = getStride(wrtSize);
		return IntStream.range(0, p.nd).map(i -> p.c[i] * s[i]).sum();
	}

	private static int[] getStride(int[] size) {
		int[] ret = new int[size.length];

		ret[size.length - 1] = 1;
		for (int i = size.length - 2; i >=0; i--)
			ret[i] = ret[i + 1] * size[i + 1];

		return ret; 
	}
}
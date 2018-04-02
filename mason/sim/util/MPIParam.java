package sim.util;

import java.util.Arrays;
import java.util.List;
//import java.util.Stack;

import mpi.*;

import sim.field.storage.GridStorage;

public class MPIParam {
	public Datatype type;
	public int idx, size;

	// TODO need to track all previously allocated datatypes and implement free() to free them all
	// private Stack<Datatype> prevDatatypes;

	public MPIParam(Datatype type, int idx, int[] size) {
		this.type = type;
		this.idx = idx;
		this.size = Arrays.stream(size).reduce(1, (a, b) -> a * b);
		// this.prevDatatypes = new Stack<Datatype>();
	}

	public MPIParam(Datatype type, int idx, int size) {
		this.type = type;
		this.idx = idx;
		this.size = size;
		// this.prevDatatypes = new Stack<Datatype>();
	}

	// public void free() {
	// 	while (!prevDatatypes.empty())
	// 		prevDatatypes.pop().free();
	// }

	public static MPIParam generate(IntHyperRect rect, IntHyperRect bound, Datatype baseType) {
		int[] bsize = bound.getSize();
		int idx = GridStorage.getFlatIdx(rect.ul.rshift(bound.ul.c), bsize);
		Datatype type = getNdArrayDatatype(rect.getSize(), baseType, bsize);

		return new MPIParam(type, idx, rect.getArea());
	}

	public static MPIParam generate(List<IntHyperRect> rects, IntHyperRect bound, Datatype baseType) {
		int cnt = rects.size();

		// If there is only one overlap, no need to combine
		if (cnt == 1)
			return generate(rects.get(0), bound, baseType);

		int[] bl = new int[cnt], displ = new int[cnt];
		Datatype[] dt = new Datatype[cnt];
		int totalSize = 0;

		for (int i = 0; i < cnt; i++) {
			IntHyperRect p = rects.get(i);
			MPIParam mp = generate(p, bound, baseType);
			bl[i] = 1;
			displ[i] = mp.idx * 8; // displacement from the start in bytes
			dt[i] = mp.type;
			totalSize += mp.size;
		}

		Datatype combined = null;
		try {
			combined = Datatype.createStruct(bl, displ, dt);
			combined.commit();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return new MPIParam(combined, 0, totalSize);
	}

	// Create Nd subarray MPI datatype
	private static Datatype getNdArrayDatatype(int[] size, Datatype base, int[] strideSize) {
		Datatype type = null;

		try {
			int sizeByte = MPI.COMM_WORLD.packSize(1, base);
			for (int i = size.length - 1; i >= 0; i--) {
				type = Datatype.createContiguous(size[i], base);
				type = Datatype.createResized(type, 0, strideSize[i] * sizeByte);
				base = type;
			}
			type.commit();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return type;
	}
}
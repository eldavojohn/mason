package sim.field.storage;

import java.util.Arrays;
import java.util.function.*;
import java.io.*;

import mpi.*;
import static mpi.MPI.slice;

import sim.util.IntHyperRect;
import sim.util.IntPoint;
import sim.util.MPIParam;

public class ObjectGridStorage<T> extends GridStorage {
	ByteArrayOutputStream out;
	ObjectOutputStream oos;

	ByteArrayInputStream in;
	ObjectInputStream ois;

	// Lambda function which accepts the size as its argument and returns a T array
	IntFunction<T[]> alloc;

	public ObjectGridStorage(IntHyperRect shape, IntFunction<T[]> allocator, int maxObjSize) {
		super(shape);
		
		alloc = allocator;
		storage = allocate(shape.getArea());

		// Create a datatype with its size to be maxObjSize
		// this will not be used for actual packing/unpacking
		// it will only be used for calculating the necessary size of packing buffer
		try {
			baseType = Datatype.createContiguous(maxObjSize, MPI.BYTE);
			baseType.commit();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	protected Object allocate(int size) {
		return alloc.apply(size);
	}

	public String toString() {
		int[] size = shape.getSize();
		T[] array = (T[])storage;
		StringBuffer buf = new StringBuffer(String.format("ObjectGridStorage<%s>-%s\n", array.getClass().getSimpleName(), shape));

		if (shape.nd == 2)
			for (int i = 0; i < size[0]; i++) {
				for (int j = 0; j < size[1]; j++)
					buf.append(String.format(" %8s ", array[i * size[1] + j]));
				buf.append("\n");
			}

		return buf.toString();
	}

	public int pack(MPIParam mp, byte[] buf, int idx) throws MPIException, IOException {
		out = new ByteArrayOutputStream();
		oos = new ObjectOutputStream(out);

		for (T obj : collect(mp)) {
			oos.writeObject(obj);
		}

		oos.flush();
		byte[] serialized = out.toByteArray();

		System.arraycopy(serialized, 0, buf, idx, serialized.length);

		oos.close();
		out.close();

		return idx + serialized.length;
	}

	public int unpack(MPIParam mp, byte[] buf, int idx, int len) throws MPIException, IOException {
		if (len == 0)
			return 0;

		in = new ByteArrayInputStream(Arrays.copyOfRange(buf, idx, idx + len));
		ois = new ObjectInputStream(in);

		T[] objs = alloc.apply(mp.size);

		for (int i = 0; i < mp.size; i++) {
			try {
				objs[i] = (T)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		return distribute(mp, objs);
	}

	private T[] collect(MPIParam mp) {
		T[] objs = alloc.apply(mp.size), stor = (T[])storage;
		int curr = 0;

		for (IntHyperRect rect : mp.rects)
			for (IntPoint p : rect)
				objs[curr++] = stor[getFlatIdx(p)];

		return objs;
	}

	private int distribute(MPIParam mp, T[] objs) {
		T[] stor = (T[])storage;
		int curr = 0;

		for (IntHyperRect rect : mp.rects)
			for (IntPoint p : rect)
				stor[getFlatIdx(p)] = (T)objs[curr++];

		return curr;
	}

	public static void main(String[] args) throws MPIException, IOException {
		MPI.Init(args);

		IntPoint p1 = new IntPoint(new int[] {0, 0});
		IntPoint p2 = new IntPoint(new int[] {5, 5});
		IntPoint p3 = new IntPoint(new int[] {1, 1});
		IntPoint p4 = new IntPoint(new int[] {4, 4});
		IntHyperRect r1 = new IntHyperRect(0, p1, p2);
		IntHyperRect r2 = new IntHyperRect(1, p3, p4);
		ObjectGridStorage<TestObj> s1 = new ObjectGridStorage<TestObj>(r1, size -> new TestObj[size], TestObj.getMaxObjectSize());
		ObjectGridStorage<TestObj> s2 = new ObjectGridStorage<TestObj>(r1, size -> new TestObj[size], TestObj.getMaxObjectSize());

		TestObj[] stor = (TestObj[])s1.getStorage();
		for (int i : new int[] {6, 12, 18})
			stor[i] = new TestObj(i);

		int maxPackSize = 25000;
		byte[] buf = new byte[maxPackSize];
		MPIParam mp = new MPIParam(r2, r1, s1.getMPIBaseType());
		int packSize = s1.pack(mp, buf, 0);
		s2.unpack(mp, buf, 0, packSize);

		TestObj[] objs = (TestObj[])s2.getStorage();
		for (TestObj obj : objs)
			System.out.print(obj + " ");
		System.out.println("");

		MPI.Finalize();
	}
}
package sim.field.storage;

import java.util.Arrays;

import mpi.*;
import static mpi.MPI.slice;

import sim.util.IntHyperRect;
import sim.util.MPIParam;

public class ObjectGridStorage<T> extends GridStorage {
	ByteArrayOutputStream out;
	ObjectOutputStream oos;

	ByteArrayInputStream in;
	ObjectInputStream ois;

	public ObjectGridStorage(IntHyperRect shape, Object initVal) {
		super(shape, initVal);
		this.baseType = MPI.BYTE;
	}

	public Object allocate(int size, Object initVal) {
		return new Object[size];
	}

	public int pack(MPIParam mp, byte[] buf, int idx) throws MPIException, IOException {
		Object[] objs = collect(mp);

		out = new ByteArrayOutputStream();
		oos = new ObjectOutputStream(out);

		for (T obj : collect(mp)) {
			if (obj == null)
				oos.writeObject(new Object());
			else
				oos.writeObject(obj);
		}

		oos.flush();
		byte[] serialized = out.toByteArray();

		System.arraycopy(serialized, 0, buf, idx, serialized.length);

		oos.close();
		out.close();

		return serialized.length;
	}

	public int unpack(MPIParam mp, byte[] buf, int idx, int len) throws MPIException, IOException {
		in = new ByteArrayInputStream(Arrays.copyOfRange(buf, idx, idx + len));
		ois = new ObjectInputStream(in);

		Object[] objs = new Object[mp.size];

		for (int i = 0; i < mp.size; i++) {
			try {
				objs[i] = (T)ois.readObject();
			} catch (ClassCastException e) {
				continue;
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		return distribute(mp, objs);
	}

	private Object[] collect(MPIParam mp) {
		Object[] objs = new Object[mp.size], stor = (Object[])storage;
		int curr = 0;

		for(IntHyperRect rect : mp.rects)
			for(IntPoint p: rect)
				objs[curr++] = stor[getFlatIdx(p)];

		return objs;
	}

	private int distribute(MPIParam mp, Object[] objs) {
		int curr = 0;
		
		for(IntHyperRect rect : mp.rects)
			for(IntPoint p: rect)
				stor[getFlatIdx(p)] = objs[curr++];

		return curr;
	}
}
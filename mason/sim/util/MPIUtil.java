package sim.util;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

import mpi.*;

import sim.field.DPartition;

// Utility class that serialize/exchange/deserialize objects using MPI
public class MPIUtil {

	// Serialize a Serializable using Java's builtin serialization and return the byte array
	private static byte[] serialize(Serializable obj) {
		byte[] buf = null;

		try (
			    ByteArrayOutputStream out = new ByteArrayOutputStream();
			    ObjectOutputStream os = new ObjectOutputStream(out)
			) {
			os.writeObject(obj);
			os.flush();
			buf = out.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return buf;
	}

	// Serialize each Serializable in objs using Java's builtin serialization
	// Concatenate their individual byte array together and return the final byte array
	// The length of each byte array will be returned through count array
	private static byte[] serialize(Serializable[] objs, int[] count) {
		byte[] buf = null;

		byte[][] objsBuf = Arrays.stream(objs).map(obj -> serialize(obj)).toArray(s -> new byte[s][]);
		for (int i = 0; i < objs.length; i++)
			count[i] = objsBuf[i].length;

		try (ByteArrayOutputStream concat = new ByteArrayOutputStream()) {
			for (byte[] objBuf : objsBuf)
				concat.write(objBuf);
			concat.flush();
			buf = concat.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return buf;
	}

	// Deserialize the object of given type T that is stored in [pos, pos + len) in buf
	private static <T extends Serializable> T deserialize(byte[] buf, int pos, int len) {
		T obj = null;

		try (
			    ByteArrayInputStream in = new ByteArrayInputStream(buf, pos, len);
			    ObjectInputStream is = new ObjectInputStream(in);
			) {
			obj = (T)is.readObject();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return obj;
	}

	// Compute the displacement according to the count
	private static int[] getDispl(int[] count) {
		return IntStream.range(0, count.length)
		       .map(x -> Arrays.stream(count).limit(x).sum())
		       .toArray();
	}

	// LP root broadcasts a Serializable object to all other LPs
	public static <T extends Serializable> T bcast(DPartition p, T obj, int root) throws MPIException {
		byte[] buf = null;

		if (p.getPid() == root)
			buf = serialize(obj);

		int[] count = new int[] {buf == null ? 0 : buf.length};

		p.getCommunicator().bcast(count, 1, MPI.INT, root);

		if (p.getPid() != root)
			buf = new byte[count[0]];

		p.getCommunicator().bcast(buf, count[0], MPI.BYTE, root);

		return MPIUtil.<T>deserialize(buf, 0, count[0]);
	}

	// TODO new api?
	public static <T extends Serializable> T bcast(T obj, int root) throws MPIException {
		byte[] buf = null;

		if (MPI.COMM_WORLD.getRank() == root)
			buf = serialize(obj);

		int[] count = new int[] {buf == null ? 0 : buf.length};

		MPI.COMM_WORLD.bcast(count, 1, MPI.INT, root);

		if (MPI.COMM_WORLD.getRank() != root)
			buf = new byte[count[0]];

		MPI.COMM_WORLD.bcast(buf, count[0], MPI.BYTE, root);

		return MPIUtil.<T>deserialize(buf, 0, count[0]);
	}

	// Reverse of gather
	public static <T extends Serializable> T scatter(DPartition p, T[] sendObjs, int root) throws MPIException {
		int pid = p.getPid(), np = p.getNumProc(), dstCount;
		int[] srcDispl = null, srcCount = new int[np];
		byte[] srcBuf = null, dstBuf;

		if (pid == root) {
			srcBuf = serialize(sendObjs, srcCount);
			srcDispl = getDispl(srcCount);
		}

		p.getCommunicator().scatter(srcCount, 1, MPI.INT, root);
		
		dstCount = srcCount[0];
		dstBuf = new byte[dstCount];

		p.getCommunicator().scatterv(srcBuf, srcCount, srcDispl, MPI.BYTE, dstBuf, dstCount, MPI.BYTE, root);

		return MPIUtil.<T>deserialize(dstBuf, 0, dstCount);
	}

	// Each LP sends the sendObj to dst
	// dst will return an ArrayList of np objects of type T
	// others will return an empty ArrayList
	public static <T extends Serializable> ArrayList<T> gather(DPartition p, T sendObj, int dst) throws MPIException {
		int np = p.getNumProc();
		int pid = p.getPid();
		int[] dstDispl, dstCount = new int[np];
		byte[] dstBuf, srcBuf;
		ArrayList<T> recvObjs = new ArrayList();

		srcBuf = serialize(sendObj);

		p.getCommunicator().gather(new int[] {srcBuf.length}, 1, MPI.INT, dstCount, 1, MPI.INT, dst);

		dstBuf = new byte[Arrays.stream(dstCount).sum()];
		dstDispl = getDispl(dstCount);

		p.getCommunicator().gatherv(srcBuf, srcBuf.length, MPI.BYTE, dstBuf, dstCount, dstDispl, MPI.BYTE, dst);

		if (pid == dst)
			for (int i = 0; i < np; i++)
				if (i == pid)
					recvObjs.add(sendObj);
				else
					recvObjs.add(MPIUtil.<T>deserialize(dstBuf, dstDispl[i], dstCount[i]));

		return recvObjs;
	}

	// Each LP contributes the sendObj
	// All the LPs will receive all the sendObjs from all the LPs returned in the ArrayList
	public static <T extends Serializable> ArrayList<T> allGather(DPartition p, T sendObj) throws MPIException {
		int np = p.getNumProc();
		int pid = p.getPid();
		int[] dstDispl, dstCount = new int[np];
		byte[] dstBuf, srcBuf;
		ArrayList<T> recvObjs = new ArrayList();

		srcBuf = serialize(sendObj);
		dstCount[pid] = srcBuf.length;

		p.getCommunicator().allGather(dstCount, 1, MPI.INT);

		dstBuf = new byte[Arrays.stream(dstCount).sum()];
		dstDispl = getDispl(dstCount);

		p.getCommunicator().allGatherv(srcBuf, srcBuf.length, MPI.BYTE, dstBuf, dstCount, dstDispl, MPI.BYTE);

		for (int i = 0; i < np; i++)
			if (i == pid)
				recvObjs.add(sendObj);
			else
				recvObjs.add(MPIUtil.<T>deserialize(dstBuf, dstDispl[i], dstCount[i]));

		return recvObjs;
	}

	// Each LP sends and receives one object to/from each of its neighbors
	// in the order that is defined in partition scheme
	public static <T extends Serializable> ArrayList<T> neighborAllToAll(DPartition p, T[] sendObjs) throws MPIException {
		int nc = sendObjs.length;
		int[] srcDispl, srcCount = new int[nc];
		int[] dstDispl, dstCount = new int[nc];
		byte[] srcBuf, dstBuf;
		ArrayList<T> recvObjs = new ArrayList();

		srcBuf = serialize(sendObjs, srcCount);
		srcDispl = getDispl(srcCount);

		p.getCommunicator().neighborAllToAll(srcCount, 1, MPI.INT, dstCount, 1, MPI.INT);

		dstBuf = new byte[Arrays.stream(dstCount).sum()];
		dstDispl = getDispl(dstCount);

		p.getCommunicator().neighborAllToAllv(srcBuf, srcCount, srcDispl, MPI.BYTE, dstBuf, dstCount, dstDispl, MPI.BYTE);

		for (int i = 0; i < nc; i++)
			recvObjs.add(MPIUtil.<T>deserialize(dstBuf, dstDispl[i], dstCount[i]));

		return recvObjs;
	}

	// neighborAllGather for primitive type data (fixed length)
	public static Object neighborAllGather(DPartition p, Object val, Datatype type) throws MPIException {
		int nc = p.getNumNeighbors();
		Object sendBuf, recvBuf;

		// Use if-else since switch-case only accepts int
		if (type == MPI.BYTE) {
			sendBuf = new byte[] {(byte)val};
			recvBuf = new byte[nc];
		} else if (type == MPI.DOUBLE) {
			sendBuf = new double[] {(double)val};
			recvBuf = new double[nc];
		} else if (type == MPI.INT) {
			sendBuf = new int[] {(int)val};
			recvBuf = new int[nc];
		} else if (type == MPI.FLOAT) {
			sendBuf = new float[] {(float)val};
			recvBuf = new float[nc];
		} else if (type == MPI.LONG) {
			sendBuf = new long[] {(long)val};
			recvBuf = new long[nc];
		} else
			throw new UnsupportedOperationException("The given MPI Datatype " + type + " is invalid / not implemented yet");

		p.getCommunicator().neighborAllGather(sendBuf, 1, type, recvBuf, 1, type);

		return recvBuf;
	}

	public static void main(String[] args) throws MPIException, IOException {
		MPI.Init(args);

		sim.field.DNonUniformPartition p = sim.field.DNonUniformPartition.getPartitionScheme(new int[] {10, 10}, true);
		p.initUniformly(null);
		p.commit();

		int[] nids = p.getNeighborIds();
		Integer[] t = new Integer[nids.length];
		for (int i = 0; i < nids.length; i++)
			t[i] = p.getPid() * 10 + nids[i];
		final int dst = 0;

		ArrayList<Integer[]> res = MPIUtil.<Integer[]>gather(p, t, dst);

		MPITest.execInOrder(x -> {
			System.out.println("gather to dst " + dst + " PID " + x);
			for (Integer[] r : res)
				for (Integer i : r)
					System.out.println(i);
		}, 100);

		Integer[] scattered = MPIUtil.<Integer[]>scatter(p, res.toArray(new Integer[0][]), dst);

		MPITest.execInOrder(x -> {
			System.out.println("scattered from src " + dst + " PID " + x);
			for (Integer i : scattered)
				System.out.println(i);
		}, 100);

		ArrayList<Integer[]> res2 = MPIUtil.<Integer[]>allGather(p, t);

		MPITest.execInOrder(x -> {
			System.out.println("allGather PID " + x);
			for (Integer[] r : res2)
				for (Integer i : r)
					System.out.println(i);
		}, 100);

		ArrayList<Integer> res3 = MPIUtil.<Integer>neighborAllToAll(p, t);

		MPITest.execInOrder(x -> {
			System.out.println("neighborAllToAll PID " + x);
			for (Integer i : res3)
				System.out.println(i);
		}, 100);

		MPI.Finalize();
	}
}
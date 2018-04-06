package sim.field;

import java.io.*;
import java.util.*;

import sim.util.*;
import ec.util.*;

import mpi.*;

public class DObjectMigratorNonUniform implements Iterable<Object> {

	int nc; // number of direct neighbors
	int[] src_count, src_displ, dst_count, dst_displ;

	HashMap<Integer, AgentOutputStream> dstMap;
	AgentOutputStream[] outputStreams;

	DPartition partition;
	int[] neighbors;

	public ArrayList<Object> objects;

	public class AgentOutputStream {
		public ByteArrayOutputStream out;
		public ObjectOutputStream os;

		public AgentOutputStream() throws IOException {
			out = new ByteArrayOutputStream();
			os = new ObjectOutputStream(out);
		}

		public void write(Object obj) throws IOException {
			os.writeObject(obj);
		}

		public byte[] toByteArray() {
			return out.toByteArray();
		}

		public int size() {
			return out.size();
		}

		public void flush() throws IOException {
			os.flush();
		}

		public void reset() throws IOException {
			os.close();
			out.close();
			out = new ByteArrayOutputStream();
			os = new ObjectOutputStream(out);
		}
	}


	public DObjectMigratorNonUniform(DPartition partition) throws MPIException, IOException {
		this.partition = partition;
		neighbors = partition.getNeighborIds();
		nc = neighbors.length;

		objects = new ArrayList<Object>();

		src_count = new int[nc];
		src_displ = new int[nc];
		dst_count = new int[nc];
		dst_displ = new int[nc];

		// outputStreams for direct neighbors
		outputStreams = new AgentOutputStream[nc];
		for (int i = 0; i < nc; i++)
			outputStreams[i] = new AgentOutputStream();

		// neighbors
		dstMap = new HashMap<Integer, AgentOutputStream>();
		for (int i = 0; i < nc; i++)
			dstMap.putIfAbsent(neighbors[i], outputStreams[i]);
	}

	@Override
	public Iterator<Object> iterator() {
		return objects.iterator();
	}

	public int size() {
		return objects.size();
	}

	public void migrate(final Object obj, final int dst) {
		MigratedObject mo = new MigratedObject(obj, dst);
		//System.out.println(String.format("[%d] dst %d %s", partition.getPid(), dst, Arrays.toString(neighbors)));
		assert dstMap.containsKey(dst);
		try {
			dstMap.get(dst).write(mo);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void sync() throws MPIException, IOException, ClassNotFoundException {
		// return if no neighbor
		if (nc < 1)
			return;

		// Prepare data
		for (int i = 0, total = 0; i < nc; i++) {
			outputStreams[i].flush();
			src_count[i] = outputStreams[i].size();
			src_displ[i] = total;
			total += src_count[i];
		}

		// Concat neighbor streams into one
		ByteArrayOutputStream objstream = new ByteArrayOutputStream();
		for (int i = 0; i < nc; i++)
			objstream.write(outputStreams[i].toByteArray());
		byte[] sendbuf = objstream.toByteArray();
		//System.out.println("partition id is "+partition.pid + ", sendbuf size is "+sendbuf.length);
		// First exchange count[] of the send byte buffers with neighbors so that we can setup recvbuf
		partition.comm.neighborAllToAll(src_count, 1, MPI.INT, dst_count, 1, MPI.INT);
		for (int i = 0, total = 0; i < nc; i++) {
			dst_displ[i] = total;
			total += dst_count[i];
		}
		byte[] recvbuf = new byte[dst_displ[nc - 1] + dst_count[nc - 1]];

		// exchange the actual object bytes
		partition.comm.neighborAllToAllv(sendbuf, src_count, src_displ, MPI.BYTE, recvbuf, dst_count, dst_displ, MPI.BYTE);

		// read and handle incoming objects
		ArrayList<MigratedObject> migrated = new ArrayList<MigratedObject>();
		for (int i = 0; i < nc; i++) {
			ByteArrayInputStream in = new ByteArrayInputStream(Arrays.copyOfRange(recvbuf, dst_displ[i], dst_displ[i] + dst_count[i]));
			ObjectInputStream is = new ObjectInputStream(in);
			boolean more = true;
			while (more) {
				try {
					migrated.add((MigratedObject)is.readObject());
				} catch (EOFException e) {
					more = false;
				}
			}
		}

		// Clear previous queues
		for (int i = 0; i < nc; i++)
			outputStreams[i].reset();

		// Handle incoming objects
		for (MigratedObject mo : migrated) {
			if (partition.pid != mo.dst) {
				assert dstMap.containsKey(mo.dst);
				dstMap.get(mo.dst).write(mo);
			} else
				objects.add(mo.obj);
		}
	}
}

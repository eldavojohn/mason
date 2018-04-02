package sim.field;

import java.io.*;
import java.util.*;
import mpi.*;
import sim.field.continuous.DContinuous2DAgent;
import sim.util.Double2D;

public class DObjectRawTypeMigrator extends DObjectMigrator {

	public SelfStreamedAgent agent;
	public ArrayList<MigratingAgent> bufferList;
	
	public DObjectRawTypeMigrator(DUniformPartition partition, SelfStreamedAgent agent) throws MPIException, IOException
	{
		super(partition);
		this.agent = agent;
		bufferList = new ArrayList<MigratingAgent>();
	}

	public void migrate(final Object obj, final int dst) {
		assert dstMap.containsKey(dst);
		try{
			// write data in wrapper, which is DContinuous2DObject
			DContinuous2DAgent wrapper = (DContinuous2DAgent)obj;
			dstMap.get(dst).os.writeInt(wrapper.destination);
			dstMap.get(dst).os.writeBoolean(wrapper.migrate);
			dstMap.get(dst).os.writeDouble(wrapper.loc.x);
			dstMap.get(dst).os.writeDouble(wrapper.loc.y);
			dstMap.get(dst).os.flush();
			// write agent
			SelfStreamedAgent selfStreamedAgent = (SelfStreamedAgent)wrapper.wrappedAgent;
			selfStreamedAgent.writeStream(dstMap.get(dst));
			// have to flush the data
			dstMap.get(dst).os.flush();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void sync() throws MPIException, IOException, ClassNotFoundException {
		// Migrate |dims| times since it need |dims| steps for an agent to migrate to a diagnol neighbor.
		for (int i = 0; i < partition.dims.length; i++)
			sync_step();
	}

	private void sync_step() throws MPIException, IOException, ClassNotFoundException {
		// Prepare data
		for (int i = 0, total = 0; i < nc; i++) {
			src_count[i] = outputStreams[i].size();
			src_displ[i] = total;
			total += src_count[i];
		}

		// Concat neighbor streams into one
		ByteArrayOutputStream objstream = new ByteArrayOutputStream();
		for (int i = 0; i < nc; i++)
			objstream.write(outputStreams[i].toByteArray());
		byte[] sendbuf = objstream.toByteArray();

		
		// First exchange count[] of the send byte buffers with neighbors so that we can setup recvbuf
		partition.comm.neighborAllToAll(src_count, 1, MPI.INT, dst_count, 1, MPI.INT);
		for (int i = 0, total = 0; i < nc; i++) {
			dst_displ[i] = total;
			total += dst_count[i];
		}
		byte[] recvbuf = new byte[dst_displ[nc - 1] + dst_count[nc - 1]];

		// exchange the actual object bytes
		partition.comm.neighborAllToAllv(sendbuf, src_count, src_displ, MPI.BYTE, recvbuf, dst_count, dst_displ, MPI.BYTE);
		
		
		
		for (int i = 0;i<nc;++i)
		{
			ByteArrayInputStream in = new ByteArrayInputStream(Arrays.copyOfRange(recvbuf, dst_displ[i], dst_displ[i] + dst_count[i]));
			ObjectInputStream is = new ObjectInputStream(in);
			boolean more = true;
			while(more) { 
				try {
					// read destination
					int dst = is.readInt();
					// read Wrapper data
					boolean migrate = is.readBoolean();
					double x = is.readDouble();
					double y = is.readDouble();
					// create the new agent
					SelfStreamedAgent newAgent = (SelfStreamedAgent) agent.clone();
					// read in the data
					newAgent.readStream(is);
					// create the wrapper
					DContinuous2DAgent wrapper = new DContinuous2DAgent(dst, newAgent, new Double2D(x, y), migrate);
					if (partition.pid != dst) {
						assert dstMap.containsKey(dst);
						bufferList.add(wrapper);
					} else
						objects.add(wrapper);
					
				} catch (EOFException e) {
					more = false;
				}
			}
		}

		// Clear previous queues
		for (int i = 0; i < nc; i++)
			outputStreams[i].reset();

		// Handling the agent in bufferList
		for (int i = 0;i<bufferList.size();++i)
		{
			DContinuous2DAgent wrapper = (DContinuous2DAgent) bufferList.get(i);
			int dst = wrapper.destination;
			dstMap.get(dst).os.writeInt(wrapper.destination);
			// write data in wrapper, which is DContinuous2DObject
			dstMap.get(dst).os.writeBoolean(wrapper.migrate);
			dstMap.get(dst).os.writeDouble(wrapper.loc.x);
			dstMap.get(dst).os.writeDouble(wrapper.loc.y);
			dstMap.get(dst).os.flush();
			SelfStreamedAgent agent = (SelfStreamedAgent)wrapper.wrappedAgent;
			agent.writeStream(dstMap.get(dst));
			dstMap.get(dst).os.flush();
		}
		bufferList.clear();
		
	}
}
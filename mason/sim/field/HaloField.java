package sim.field;

import java.io.*;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.stream.IntStream;

import mpi.*;

import sim.field.storage.GridStorage;
import sim.util.IntHyperRect;
import sim.util.IntPoint;
import sim.util.IntPointGenerator;
import sim.util.MPIParam;
import sim.util.MPIUtil;

// TODO refactor HaloField to accept
// continuous: double, int, object

public abstract class HaloField implements RemoteField {

	protected int nd, numNeighbors;
	protected int[] aoi, fieldSize, haloSize;

	protected IntHyperRect world, haloPart, origPart, privPart;
	protected Neighbor[] neighbors;
	protected GridStorage field;
	protected DPartition ps;
	protected Comm comm;
	protected Datatype MPIBaseType;

	protected String myRemoteName;
	protected Registry origRegistry, registry;
	protected RemoteField[] remoteFields;
	protected final int RMI_REGISTRY_PORT = 1099;

	public HaloField(DPartition ps, int[] aoi, GridStorage stor) {
		this.ps = ps;
		this.aoi = aoi;
		this.field = stor;

		initRemote(0);

		ps.registerPreCommit(new Runnable() {
			public void run() {
				try {
					sync();
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
		});

		ps.registerPostCommit(new Runnable() {
			public void run() {
				try {
					reload();
					sync();
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
		});

		reload();
	}

	public void reload() {
		nd = ps.getNumDim();
		comm = ps.getCommunicator();
		fieldSize = ps.getFieldSize();
		world = ps.getField();
		origPart = ps.getPartition();

		// Get the partition representing halo and local area by expanding the original partition by aoi at each dimension
		haloPart = origPart.resize(aoi);
		haloSize = haloPart.getSize();

		MPIBaseType = field.getMPIBaseType();
		field.reshape(haloPart);

		// Get the partition representing private area by shrinking the original partition by aoi at each dimension
		privPart = origPart.resize(Arrays.stream(aoi).map(x -> -x).toArray());

		// Get the neighbors and create Neighbor objects
		neighbors = Arrays.stream(ps.getNeighborIds())
		            .mapToObj(x -> new Neighbor(ps.getPartition(x)))
		            .toArray(size -> new Neighbor[size]);
		numNeighbors = neighbors.length;
	}

	protected void initRemote(int registryHostId) {
		// Generate a random name
		myRemoteName = UUID.randomUUID().toString();

		try {
			String hostAddr = null;

			// one LP creates a rmiregistry
			if (ps.getPid() == registryHostId) {
				hostAddr = InetAddress.getLocalHost().getHostAddress();
				System.out.printf("Starting rmiregistry in %s on port %d\n", hostAddr, RMI_REGISTRY_PORT);
				origRegistry = LocateRegistry.createRegistry(RMI_REGISTRY_PORT);
			}

			// Broadcast that LP's ip address to other LPs so they can connect to the registry
			hostAddr = MPIUtil.<String>bcast(ps, hostAddr, registryHostId);
			registry = LocateRegistry.getRegistry(hostAddr, RMI_REGISTRY_PORT);

			// Create a RMI server and register it in the registry
			RemoteField rf = (RemoteField) UnicastRemoteObject.exportObject(this, 0);
			registry.bind(myRemoteName, rf);

			// Exchange the names with all other LPs so that each LP can create RemoteField clients for all other LPs
			ArrayList<String> names = MPIUtil.<String>allGather(ps, myRemoteName);
			remoteFields = new RemoteField[ps.np];
			for (int i = 0; i < ps.np; i++)
				remoteFields[i] = (RemoteField)registry.lookup(names.get(i));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	// Need to call teardown to de-register the RMI registry stuff
	// so that the code won't stuch in the end
	// TODO move the RMI stuff to a separate class
	// TODO hook this to MPI finalize so that this will be called before exit
	public void teardownRemote(int registryHostId) {
		try {
			registry.unbind(myRemoteName);
			UnicastRemoteObject.unexportObject(this, true);
			if (origRegistry != null)
				UnicastRemoteObject.unexportObject(origRegistry, true);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	// TODO make a copy of the storage which will be used by the remote field access
	protected Serializable getFromRemote(IntPoint p) {
		Serializable ret = null;
		int pid = ps.toPartitionId(p);

		try {
			ret = remoteFields[pid].getRMI(p);
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return ret;
	}

	public GridStorage getStorage() {
		return field;
	}

	// Various stabbing queries
	public boolean inGlobal(IntPoint p) {
		return IntStream.range(0, nd).allMatch(i -> p.c[i] >= 0 && p.c[i] < fieldSize[i]);
	}

	public boolean inLocal(IntPoint p) {
		return origPart.contains(p);
	}

	public boolean inPrivate(IntPoint p) {
		return privPart.contains(p);
	}

	public boolean inLocalAndHalo(IntPoint p) {
		return haloPart.contains(p);
	}

	public boolean inShared(IntPoint p) {
		return inLocal(p) && !inPrivate(p);
	}

	public boolean inHalo(IntPoint p) {
		return inLocalAndHalo(p) && !inLocal(p);
	}

	public IntPoint toLocalPoint(IntPoint p) {
		return p.rshift(haloPart.ul.c);
	}

	public IntPoint toToroidal(IntPoint p) {
		return p.toToroidal(world);
	}

	public int toToroidal(int x, int dim) {
		int s = fieldSize[dim];
		if (x >= s )
			return x - s;
		else if (x < 0)
			return x + s;
		return x;
	}

	public int stx(final int x) {
		return toToroidal(x, 0);
	}

	public int sty(final int y) {
		return toToroidal(y, 1);
	}

	public void sync() throws MPIException {
		Serializable[] sendObjs = new Serializable[numNeighbors];
		for (int i = 0; i < numNeighbors; i++)
			sendObjs[i] = field.pack(neighbors[i].sendParam);

		ArrayList<Serializable> recvObjs = MPIUtil.<Serializable>neighborAllToAll(ps, sendObjs);

		for (int i = 0; i < numNeighbors; i++)
			field.unpack(neighbors[i].recvParam, recvObjs.get(i));
	}

	public void collect(int dst, GridStorage fullField) throws MPIException {
		Serializable sendObj = field.pack(new MPIParam(origPart, haloPart, MPIBaseType));

		ArrayList<Serializable> recvObjs = MPIUtil.<Serializable>gather(ps, sendObj, dst);

		if (ps.getPid() == dst)
			for (int i = 0; i < ps.getNumProc(); i++)
				fullField.unpack(new MPIParam(ps.getPartition(i), world, MPIBaseType), recvObjs.get(i));
	}

	public void distribute(int src, GridStorage fullField) throws MPIException {
		Serializable[] sendObjs = new Serializable[ps.getNumProc()];

		if (ps.getPid() == src)
			for (int i = 0; i < ps.getNumProc(); i++)
				sendObjs[i] = fullField.pack(new MPIParam(ps.getPartition(i), world, MPIBaseType));

		Serializable recvObj = MPIUtil.<Serializable>scatter(ps, sendObjs, src);
		field.unpack(new MPIParam(origPart, haloPart, MPIBaseType), recvObj);

		// Sync the halo
		sync();
	}

	public String toString() {
		return String.format("PID %d Storage %s", ps.getPid(), field);
	}

	// Helper class to organize neighbor-related data structures and methods
	class Neighbor {
		int pid;
		MPIParam sendParam, recvParam;

		public Neighbor(IntHyperRect neighborPart) {
			pid = neighborPart.id;
			ArrayList<IntHyperRect> sendOverlaps = generateOverlaps(origPart, neighborPart.resize(aoi));
			ArrayList<IntHyperRect> recvOverlaps = generateOverlaps(haloPart, neighborPart);

			assert sendOverlaps.size() == recvOverlaps.size();

			// Sort these overlaps so that they corresponds to each other
			Collections.sort(sendOverlaps);
			Collections.sort(recvOverlaps, Collections.reverseOrder());

			sendParam = new MPIParam(sendOverlaps, haloPart, MPIBaseType);
			recvParam = new MPIParam(recvOverlaps, haloPart, MPIBaseType);
		}

		private ArrayList<IntHyperRect> generateOverlaps(IntHyperRect p1, IntHyperRect p2) {
			ArrayList<IntHyperRect> overlaps = new ArrayList<IntHyperRect>();

			if (ps.isToroidal())
				for (IntPoint p : IntPointGenerator.getLayer(nd, 1)) {
					IntHyperRect sp = p2.shift(IntStream.range(0, nd).map(i -> p.c[i] * fieldSize[i]).toArray());
					if (p1.isIntersect(sp))
						overlaps.add(p1.getIntersection(sp));
				}
			else
				overlaps.add(p1.getIntersection(p2));

			return overlaps;
		}
	}
}

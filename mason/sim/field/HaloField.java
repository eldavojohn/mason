package sim.field;

import java.io.*;
import java.rmi.RemoteException;
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

	protected RemoteProxy proxy;

	public HaloField(DPartition ps, int[] aoi, GridStorage stor) {
		this.ps = ps;
		this.aoi = aoi;
		this.field = stor;

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

		// init variables that don't change with the partition scheme
		nd = ps.getNumDim();
		world = ps.getField();
		fieldSize = ps.getFieldSize();
		MPIBaseType = field.getMPIBaseType();

		// init variables that may change with the partition scheme
		reload();
	}

	public void reload() {
		comm = ps.getCommunicator();
		origPart = ps.getPartition();

		// Get the partition representing halo and local area by expanding the original partition by aoi at each dimension
		haloPart = origPart.resize(aoi);
		haloSize = haloPart.getSize();

		field.reshape(haloPart);

		// Get the partition representing private area by shrinking the original partition by aoi at each dimension
		privPart = origPart.resize(Arrays.stream(aoi).map(x -> -x).toArray());

		// Get the neighbors and create Neighbor objects
		neighbors = Arrays.stream(ps.getNeighborIds())
		            .mapToObj(x -> new Neighbor(ps.getPartition(x)))
		            .toArray(size -> new Neighbor[size]);
		numNeighbors = neighbors.length;
	}

	public void initRemote() {
		proxy = new RemoteProxy(ps, this);
	}

	// TODO make a copy of the storage which will be used by the remote field access
	protected Serializable getFromRemote(IntPoint p) {
		Serializable ret = null;
		int pid = ps.toPartitionId(p);

		try {
			ret = proxy.getField(pid).getRMI(p);
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (NullPointerException e) {
			throw new IllegalArgumentException("Remote Proxy is not initialized");
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

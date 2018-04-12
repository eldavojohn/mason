package sim.field;

import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

import mpi.*;

import sim.field.storage.GridStorage;
import sim.util.IntHyperRect;
import sim.util.IntPoint;
import sim.util.MPIParam;
import sim.util.MPIUtil;

// TODO refactor HaloField to accept
// continuous: double, int, object

public abstract class HaloField {

	protected int nd, numNeighbors;
	protected int[] aoi, fieldSize, haloSize;

	protected IntHyperRect world, haloPart, origPart, privPart;
	protected Neighbor[] neighbors;
	protected GridStorage field;
	protected DPartition ps;
	protected Comm comm;
	protected Datatype MPIBaseType;

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
				// iterate throw all {-1, 0, 1}^nd possible combinations
				for (int k = 0; k < (int)Math.pow(3, nd); k++) {
					final int idx = k;
					int[] offsets = IntStream.range(0, nd).map(i -> (1 - idx / (int)Math.pow(3, nd - i - 1) % 3) * fieldSize[i]).toArray();
					IntHyperRect sp = p2.shift(offsets);
					if (p1.isIntersect(sp))
						overlaps.add(p1.getIntersection(sp));
				}
			else
				overlaps.add(p1.getIntersection(p2));

			return overlaps;
		}
	}
}

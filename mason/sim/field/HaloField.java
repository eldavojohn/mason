package sim.field;

import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

import mpi.*;

import sim.field.storage.GridStorage;
import sim.field.storage.DoubleGridStorage;
import sim.util.IntHyperRect;
import sim.util.IntPoint;
import sim.util.MovingAverage;
import sim.util.MPIParam;
import sim.util.Timing;

// TODO refactor HaloField to accept
// continuous: double, int, object

public abstract class HaloField {

	protected int nd, numNeighbors, maxSendSize;
	protected int[] aoi, fieldSize, haloSize, partSize;

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

		reload();
	}

	public void reload() {
		nd = ps.getNumDim();
		comm = ps.getCommunicator();
		fieldSize = ps.getFieldSize();
		world = ps.getField();
		origPart = ps.getPartition();
		partSize = origPart.getSize();

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

		// Get the max size for one exchange, which is the area of the halo area x 2 (possible overlap)
		try {
			maxSendSize = comm.packSize((haloPart.getArea() - origPart.getArea()) * 2, MPIBaseType);
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}
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

	public void sync() throws MPIException, IOException {
		byte[] sendbuf = new byte[maxSendSize], recvbuf = new byte[maxSendSize];
		int[]  sendPos = new int[numNeighbors], recvPos = new int[numNeighbors];
		int[]  sendCnt = new int[numNeighbors], recvCnt = new int[numNeighbors];

		// Pack data into 1-d byte array
		for (int i = 0, lastPos = 0; i < numNeighbors; i++) {
			sendPos[i] = lastPos;
			lastPos = field.pack(neighbors[i].sendParam, sendbuf, lastPos);
			sendCnt[i] = lastPos - sendPos[i];
		}

		// Exchange the amount of data to be exchanged
		comm.neighborAllToAll(sendCnt, 1, MPI.INT, recvCnt, 1, MPI.INT);

		for (int i = 1; i < numNeighbors; i++)
			recvPos[i] = recvPos[i - 1] + recvCnt[i - 1];

		// Exchange actual data with neighbors
		// TODO switch to neighborAlltoAllw (so no need to pack/unpack)
		// once it is implemented in OpenMPI Java bindings
		comm.neighborAllToAllv(sendbuf, sendCnt, sendPos, MPI.BYTE, recvbuf, recvCnt, recvPos, MPI.BYTE);

		// Unpack into the field
		for (int i = 0; i < numNeighbors; i++)
			field.unpack(neighbors[i].recvParam, recvbuf, recvPos[i], recvCnt[i]);

		// Exchange aux data, e.g., runtime data for load balancing
		double runtime;
		try {
			runtime = Timing.get(Timing.LB_RUNTIME).getMovingAverage();
		} catch (NoSuchElementException e) {
			return; // not set - no need to exchange
		}

		double[] avgSendBuf = new double[] {runtime};
		double[] avgRecvBuf = new double[numNeighbors];

		comm.neighborAllGather(avgSendBuf, 1, MPI.DOUBLE, avgRecvBuf, 1, MPI.DOUBLE);

		for (int i = 0; i < numNeighbors; i++)
			neighbors[i].avgRuntime = avgRecvBuf[i];
	}

	// TODO refactor the performance measurements into a separate class
	public HashMap<Integer, Double> getRuntimes() {
		HashMap<Integer, Double> ret = new HashMap<Integer, Double>();
		Arrays.stream(neighbors).forEach(x -> ret.put(x.pid, x.avgRuntime));
		return ret;
	}

	public void collect(int dst, GridStorage fullField) throws MPIException, IOException {
		int[] displ = null, count = null;
		byte[] sendbuf = null, recvbuf = null;
		int sendSize, pid = ps.getPid(), np = ps.getNumProc();

		if (pid == dst) {
			count = new int[np];
			displ = new int[np];
		}

		// Everyone pack the data into byte array
		sendbuf = new byte[comm.packSize(origPart.getArea(), MPIBaseType)];
		sendSize = field.pack(new MPIParam(origPart, haloPart, MPIBaseType), sendbuf, 0);

		// First gather the size of data to be exchanged
		comm.gather(new int[] {sendSize}, 1, MPI.INT, count, 1, MPI.INT, dst);

		// Dst compute the displacement array and init the recvbuf
		if (pid == dst) {
			for (int i = 1; i < np; i++)
				displ[i] = displ[i - 1] + count[i - 1];
			recvbuf = new byte[Arrays.stream(count).sum()];
		}

		// Now gather the actual data
		comm.gatherv(sendbuf, sendSize, MPI.BYTE, recvbuf, count, displ, MPI.BYTE, dst);

		// Dst unpack the data into fullField
		if (pid == dst)
			for (int i = 0; i < np; i++)
				fullField.unpack(new MPIParam(ps.getPartition(i), world, MPIBaseType), recvbuf, displ[i], count[i]);
	}

	public String toString() {
		return String.format("PID %d Storage %s", ps.getPid(), field);
	}

	// Helper class to organize neighbor-related data structures and methods
	class Neighbor {
		int pid;
		MPIParam sendParam, recvParam;

		// TODO refactor the performance measurements into a separate class
		double avgRuntime;

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

package sim.field;

import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

import mpi.*;

import sim.util.IntPoint;
import sim.util.IntHyperRect;
import sim.util.MovingAverage;

import sim.util.MPIParam;
import sim.field.storage.DoubleGridStorage;

// TODO refactor HaloField to accept
// grid: double, int, object
// continuous: double, int, object
// make halofield abstract and let subclasses like DDoubleGrid2D to implement functions including
// get/set/...

// TODO remove HaloFieldContinuous and HaloFieldGrid
// and change all dependencies to this class
// once the above TODO is done

public class HaloField {

	DoubleGridStorage field;
	double initVal;

	int nd, numNeighbors, maxSendSize;
	int[] fieldSize, haloSize, partSize;
	public int[] aoi;
	IntHyperRect haloPart, origPart, privPart;
	Neighbor[] neighbors;

	Comm comm;
	DPartition ps;

	Datatype MPIBaseType;

	// TODO refactor the performance measurements into a separate class
	long prevts;
	MovingAverage avg;

	public HaloField(DPartition ps, int[] aoi, double initVal) {
		this.ps = ps;
		this.aoi = aoi;
		this.initVal = initVal;

		reload();

		prevts = System.nanoTime();
		avg = new MovingAverage(10);
	}

	public void reload() {
		nd = ps.getNumDim();
		comm = ps.getCommunicator();
		fieldSize = ps.getFieldSize();
		origPart = ps.getPartition();
		partSize = origPart.getSize();

		// Get the partition representing halo and local area by expanding the original partition by aoi at each dimension
		haloPart = origPart.resize(aoi);
		haloSize = haloPart.getSize();

		// Init local storage if field is null
		// otherwise re-arrange the data based on the old and new partiton
		if (field == null) {
			field = new DoubleGridStorage(haloPart, initVal);
			MPIBaseType = field.getMPIBaseType();
		} else
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

	public double[] getField() {
		return (double[])field.getStorage();
	}

	public final double get(final IntPoint p) {
		// In global
		if (!inGlobal(p))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of global boundary", ps.getPid(), p.toString()));

		// In this partition and its surrounding ghost cells
		if (!inLocalAndHalo(p))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of local boundary", ps.getPid(), p.toString()));

		return ((double[])(field.getStorage()))[field.getFlatIdx(p)];
	}

	public final void set(final IntPoint p, final double val) {
		// In global
		if (!inGlobal(p))
			throw new IllegalArgumentException(String.format("PID %d set %s is out of global boundary", ps.getPid(), p.toString()));

		// In this partition but not in ghost cells
		if (!inLocal(p))
			throw new IllegalArgumentException(String.format("PID %d set %s is out of local boundary", ps.getPid(), p.toString()));

		((double[])(field.getStorage()))[field.getFlatIdx(p)] = val;
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
		return inGlobal(p) && haloPart.contains(p);
	}

	public boolean inShared(IntPoint p) {
		return inLocal(p) && !inPrivate(p);
	}

	public boolean inHalo(IntPoint p) {
		return inLocalAndHalo(p) && !inLocal(p);
	}

	// Get the corresponding index in the global flatted 1-d array of the given point
	// The given point is expected to be a global one
	private int getFlatIdxGlobal(IntPoint p) {
		return DoubleGridStorage.getFlatIdx(p, fieldSize);
	}

	public void sync() throws MPIException {
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
		long currts = System.nanoTime();
		double[] avgSendBuf = new double[] {avg.next((double)(currts - prevts))};
		double[] avgRecvBuf = new double[numNeighbors];

		comm.neighborAllGather(avgSendBuf, 1, MPI.DOUBLE, avgRecvBuf, 1, MPI.DOUBLE);

		for (int i = 0; i < numNeighbors; i++)
			neighbors[i].avgRuntime = avgRecvBuf[i];
		prevts = currts;
	}

	// TODO refactor the performance measurements into a separate class
	public HashMap<Integer, Double> getRuntimes() {
		HashMap<Integer, Double> ret = new HashMap<Integer, Double>();

		ret.put(ps.getPid(), avg.average());
		Arrays.stream(neighbors).forEach(x -> ret.put(x.pid, x.avgRuntime));

		return ret;
	}

	public double[] collect(int dst) throws MPIException {
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
		comm.gatherv(sendbuf, sendbuf.length, MPI.BYTE, recvbuf, count, displ, MPI.BYTE, dst);

		// Dst unpack the data into fullField
		if (pid == dst) {
			IntHyperRect fullPart = new IntHyperRect(-1, new IntPoint(new int[nd]), new IntPoint(fieldSize));
			DoubleGridStorage fullField = new DoubleGridStorage(fullPart, initVal);
			for (int i = 0; i < np; i++)
				fullField.unpack(new MPIParam(ps.getPartition(i), fullPart, MPIBaseType), recvbuf, displ[i], count[i]);
			return (double[])fullField.getStorage();
		}

		return null;
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

	public static void main(String args[]) throws MPIException, InterruptedException {
		int[] size = new int[] {10, 10};
		int[] aoi = new int[] {1, 1};
		int[] want, got;

		MPI.Init(args);

		/**
		* Create the following partition scheme
		*
		*	 0		4		7			10
		*	0 ---------------------------
		*	  |				|			|
		*	  |		P0		|	  P1	|
		*	5 |-------------------------|
		*	  |		|					|
		*	  |	P2	|		 P3			|
		*  10 ---------------------------
		*
		**/

		DNonUniformPartition p = new DNonUniformPartition(size, true);

		assert p.np == 4;

		p.insertPartition(new IntHyperRect(0, new IntPoint(new int[] {0, 0}), new IntPoint(new int[] {5, 7})));
		p.insertPartition(new IntHyperRect(1, new IntPoint(new int[] {0, 7}), new IntPoint(new int[] {5, 10})));
		p.insertPartition(new IntHyperRect(2, new IntPoint(new int[] {5, 0}), new IntPoint(new int[] {10, 4})));
		p.insertPartition(new IntHyperRect(3, new IntPoint(new int[] {5, 4}), new IntPoint(new int[] {10, 10})));
		p.setMPITopo();

		HaloField hf = new HaloField(p, aoi, p.pid);

		assert hf.inGlobal(new IntPoint(new int[] {5, 8}));
		assert !hf.inGlobal(new IntPoint(new int[] { -3, 0}));
		assert !hf.inGlobal(new IntPoint(new int[] {7, 240}));

		if (p.pid == 0) {
			// TODO complete those tests
			// assert hf.inLocal(new IntPoint(new int[] {0, 99}));
			// assert !hf.inLocal(new IntPoint(new int[] {50, 0}));

			// assert hf.inPrivate(new IntPoint(new int[] {aoi[0], aoi[1]}));
			// assert !hf.inPrivate(new IntPoint(new int[] {50, 0}));
			// assert !hf.inPrivate(new IntPoint(new int[] {0, 99}));

			// assert hf.inShared(new IntPoint(new int[] {49, 99}));
			// assert !hf.inShared(new IntPoint(new int[] {50, 100}));
			// assert !hf.inShared(new IntPoint(new int[] {25, 50}));

			// assert hf.inHalo(new IntPoint(new int[] {0, 100}));
		}

		hf.sync();

		printHaloField(hf, p);

		double[] all = hf.collect(0);

		if (p.pid == 0) {
			System.out.println("\nEntire Field...\n");
			for (int i = 0; i < p.size[0]; i++) {
				for (int j = 0; j < p.size[1]; j++)
					System.out.printf("%.1f\t", all[i * p.size[1] + j]);
				System.out.printf("\n");
			}
			System.out.printf("\n");
		}

		MPI.COMM_WORLD.barrier();

		if (p.pid == 0)
			System.out.println("\nTest Repartitioning #1...\n");
		/**
		* Change the partition to the following
		*
		*	 0		   5 6	 			10
		*	0 ---------------------------
		*	  |			 | <-			|
		*	  |		P0	 | <- 	 P1		|
		*	5 |-------------------------|
		*	  |		 ->|				|
		*	  |	P2	 ->|		 P3		|
		*  10 ---------------------------
		*
		**/
		p.updatePartition(new IntHyperRect(0, new IntPoint(new int[] {0, 0}), new IntPoint(new int[] {5, 6})));
		p.updatePartition(new IntHyperRect(1, new IntPoint(new int[] {0, 6}), new IntPoint(new int[] {5, 10})));
		p.updatePartition(new IntHyperRect(2, new IntPoint(new int[] {5, 0}), new IntPoint(new int[] {10, 5})));
		p.updatePartition(new IntHyperRect(3, new IntPoint(new int[] {5, 5}), new IntPoint(new int[] {10, 10})));
		p.setMPITopo();

		hf.reload();
		hf.sync();

		printHaloField(hf, p);

		if (p.pid == 0)
			System.out.println("\nTest Repartitioning #2...\n");
		/**
		* Change the partition to the following
		*
		*	 0		     6	 			10
		*	0 ---------------------------
		*	  |			 | 				|
		*	  |		P0	 | 	 	 P1		|
		*	5 |-------------------------|
		*	  |		  -> |				|
		*	  |	P2	  -> |		 P3		|
		*  10 ---------------------------
		*
		**/
		p.updatePartition(new IntHyperRect(2, new IntPoint(new int[] {5, 0}), new IntPoint(new int[] {10, 6})));
		p.updatePartition(new IntHyperRect(3, new IntPoint(new int[] {5, 6}), new IntPoint(new int[] {10, 10})));
		p.setMPITopo();

		hf.reload();
		hf.sync();

		printHaloField(hf, p);

		if (p.pid == 0)
			System.out.println("\nTest Repartitioning #3...\n");
		/**
		* Change the partition to the following
		*
		*	 0		  	 6	 			10
		*	0 ---------------------------
		*	  |		P0	 | 				|
		*	4 |----------| 	 	P1		|
		*	  |		^^	 |	||			|
		*	6 |		 	 |--------------|
		*	  |	P2	 	 |		 P3		|
		*  10 ---------------------------
		*
		**/
		p.updatePartition(new IntHyperRect(0, new IntPoint(new int[] {0, 0}), new IntPoint(new int[] {4, 6})));
		p.updatePartition(new IntHyperRect(1, new IntPoint(new int[] {0, 6}), new IntPoint(new int[] {6, 10})));
		p.updatePartition(new IntHyperRect(2, new IntPoint(new int[] {4, 0}), new IntPoint(new int[] {10, 6})));
		p.updatePartition(new IntHyperRect(3, new IntPoint(new int[] {6, 6}), new IntPoint(new int[] {10, 10})));
		p.setMPITopo();

		hf.reload();
		hf.sync();

		printHaloField(hf, p);

		MPI.Finalize();
	}

	private static void printHaloField(HaloField hf, DNonUniformPartition p) throws MPIException, InterruptedException {
		MPI.COMM_WORLD.barrier();

		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);

		double[] field = (double[])hf.field.getStorage();

		System.out.println("PID " + p.pid + " data: ");
		int w = p.getPartition().getSize()[0] + 2 * hf.aoi[0];
		int h = p.getPartition().getSize()[1] + 2 * hf.aoi[1];
		for (int i = 0; i < w; i++) {
			if (i == 1 || i == w - 1)
				System.out.println("");
			for (int j = 0; j < h; j++) {
				if (j == 1 || j == h - 1)
					System.out.printf("   \t");
				System.out.printf("%.1f\t", field[i * h + j]);
			}
			System.out.printf("\n");
		}
		System.out.printf("\n");
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);

		MPI.COMM_WORLD.barrier();
	}
}

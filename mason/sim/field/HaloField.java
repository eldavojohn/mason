package sim.field;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.IntStream;

import mpi.*;
import static mpi.MPI.slice;

import sim.util.IntPoint;
import sim.util.IntHyperRect;
import sim.util.MovingAverage;

// TODO refactor HaloField to accept
// grid: double, int, object
// continuous: double, int, object

// TODO remove HaloFieldContinuous and HaloFieldGrid
// and change all dependencies to this class
// once the above TODO is done

public class HaloField {

	// 1-D array to hold the local partition and its halo area
	double[] field;
	double initVal;

	int nd, numNeighbors, maxSendSize;
	int[] fieldSize, haloSize, partSize;
	public int[] aoi;
	IntHyperRect haloPart, origPart, privPart;
	Neighbor[] neighbors;

	Comm comm;
	DPartition ps;

	Datatype MPIBaseType = MPI.DOUBLE;

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
		IntHyperRect prevHaloPart = haloPart;
		double[] prevField = field;

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
		field = new double[haloPart.getArea()];
		Arrays.fill(field, initVal);
		if (prevField != null && haloPart.isIntersect(prevHaloPart)) {
			IntHyperRect overlap = haloPart.getIntersection(prevHaloPart);

			Datatype from_dt = getNdArrayDatatype(overlap.getSize(), MPIBaseType, prevHaloPart.getSize());
			Datatype to_dt = getNdArrayDatatype(overlap.getSize(), MPIBaseType, haloPart.getSize());

			int from_idx = getFlatIdx(
			                   overlap.ul.rshift(prevHaloPart.ul.c),
			                   prevHaloPart.getSize()
			               );
			int to_idx = getFlatIdx(
			                 overlap.ul.rshift(haloPart.ul.c),
			                 haloPart.getSize()
			             );

			try {
				byte[] buf = new byte[comm.packSize(overlap.getArea(), MPIBaseType)];

				comm.pack(slice(prevField, from_idx), 1, from_dt, buf, 0);
				comm.unpack(buf, 0, slice(field, to_idx), 1, to_dt);

				from_dt.free();
				to_dt.free();
			} catch (MPIException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

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
		return field;
	}

	public final double get(final IntPoint p) {
		// In global
		if (!inGlobal(p))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of global boundary", ps.getPid(), p.toString()));

		// In this partition and its surrounding ghost cells
		if (!inLocalAndHalo(p))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of local boundary", ps.getPid(), p.toString()));

		return field[getFlatIdxLocal(toLocalPoint(p))];
	}

	public final void set(final IntPoint p, final double val) {
		// In global
		if (!inGlobal(p))
			throw new IllegalArgumentException(String.format("PID %d set %s is out of global boundary", ps.getPid(), p.toString()));

		// In this partition but not in ghost cells
		if (!inLocal(p))
			throw new IllegalArgumentException(String.format("PID %d set %s is out of local boundary", ps.getPid(), p.toString()));

		field[getFlatIdxLocal(toLocalPoint(p))] = val;
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

	// Get the corresponding index in the local flatted 1-d array of the given point
	// The given point is expected to be a local one
	private int getFlatIdxLocal(IntPoint p) {
		return getFlatIdx(p, haloSize);
	}

	// Get the corresponding index in the global flatted 1-d array of the given point
	// The given point is expected to be a global one
	private int getFlatIdxGlobal(IntPoint p) {
		return getFlatIdx(p, fieldSize);
	}

	// Get the flatted index with respect to the given size
	private int getFlatIdx(IntPoint p, int[] wrtSize) {
		return IntStream.range(0, nd).map(i -> p.c[i] * stride(i, wrtSize)).sum();
	}

	private int stride(int dim, final int[] size) {
		return IntStream.range(1, nd - dim).reduce(1, (x, i) -> x * size[i]);
	}

	private IntPoint toLocalPoint(IntPoint p) {
		return p.rshift(origPart.ul.c).shift(aoi);
	}

	public void sync() throws MPIException {
		byte[] sendbuf = new byte[maxSendSize], recvbuf = new byte[maxSendSize];
		int[]  sendPos = new int[numNeighbors], recvPos = new int[numNeighbors];
		int[]  sendCnt = new int[numNeighbors], recvCnt = new int[numNeighbors];

		// Pack data into 1-d byte array
		for (int i = 0, lastPos = 0; i < numNeighbors; i++) {
			sendPos[i] = lastPos;
			lastPos = comm.pack(slice(field, neighbors[i].sendParam.idx), 1, neighbors[i].sendParam.type, sendbuf, lastPos);
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
			comm.unpack(recvbuf, recvPos[i], slice(field, neighbors[i].recvParam.idx), 1, neighbors[i].recvParam.type);

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

	private byte[] packPartition() throws MPIException {
		byte[] buf = new byte[comm.packSize(origPart.getArea(), MPIBaseType)];
		int idx = getFlatIdxLocal(new IntPoint(aoi));
		Datatype type = getNdArrayDatatype(partSize, MPIBaseType, haloSize);
		int	actualSize = comm.pack(slice(field, idx), 1, type, buf, 0);

		// Return the truncated array
		return Arrays.copyOf(buf, actualSize);
	}

	// Create Nd subarray MPI datatype
	private Datatype getNdArrayDatatype(int[] size, Datatype base, int[] strideSize) {
		Datatype type = null;

		try {
			int sizeByte = MPI.COMM_WORLD.packSize(1, base);
			for (int i = size.length - 1; i >= 0; i--) {
				type = Datatype.createContiguous(size[i], base);
				type = Datatype.createResized(type, 0, strideSize[i] * sizeByte);
				base = type;
			}
			type.commit();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return type;
	}

	public double[] collect(int dst) throws MPIException {
		double[] fullField = null;
		int[] displ = null, count = null;
		byte[] sendbuf = null, recvbuf = null;
		int pid = ps.getPid(), np = ps.getNumProc();

		if (pid == dst) {
			count = new int[np];
			displ = new int[np];
			fullField = new double[Arrays.stream(fieldSize).reduce(1, (x, y) -> x * y)];
		}

		// Everyone pack the data into byte array
		sendbuf = packPartition();

		// First gather the size of data to be exchanged
		comm.gather(new int[] {sendbuf.length}, 1, MPI.INT, count, 1, MPI.INT, dst);

		// Dst compute the displacement array and init the recvbuf
		if (pid == dst) {
			for (int i = 1; i < np; i++)
				displ[i] = displ[i - 1] + count[i - 1];
			recvbuf = new byte[Arrays.stream(count).sum()];
		}

		// Now gather the actual data
		comm.gatherv(sendbuf, sendbuf.length, MPI.BYTE, recvbuf, count, displ, MPI.BYTE, dst);

		// Dst unpack the data into fullField
		if (pid == dst)
			for (int i = 0; i < np; i++) {
				IntHyperRect part = ps.getPartition(i);
				int idx = getFlatIdxGlobal(part.ul);
				Datatype type = getNdArrayDatatype(part.getSize(), MPIBaseType, fieldSize);
				comm.unpack(recvbuf, displ[i], slice(fullField, idx), 1, type);
			}

		return fullField;
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

			sendParam = generateCombinedParam(sendOverlaps);
			recvParam = generateCombinedParam(recvOverlaps);
		}

		private MPIParam generateCombinedParam(ArrayList<IntHyperRect> overlaps) {
			int cnt = overlaps.size();

			// If there is only one overlap, no need to combine
			if (cnt == 1)
				return generateMPIParam(overlaps.get(0));

			int[] bl = new int[cnt], displ = new int[cnt];
			Datatype[] dt = new Datatype[cnt];

			for (int i = 0; i < cnt; i++) {
				IntHyperRect p = overlaps.get(i);
				MPIParam mp = generateMPIParam(p);
				bl[i] = 1;
				displ[i] = mp.idx * 8; // displacement from the start in bytes
				dt[i] = mp.type;
			}

			Datatype combined = null;
			try {
				combined = Datatype.createStruct(bl, displ, dt);
				combined.commit();
			} catch (MPIException e) {
				e.printStackTrace();
				System.exit(-1);
			}

			return new MPIParam(combined, 0, null);
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

		private MPIParam generateMPIParam(IntHyperRect overlap) {
			int[] size = overlap.getSize();
			int idx = getFlatIdxLocal(toLocalPoint(overlap.ul));
			Datatype type = getNdArrayDatatype(size, MPIBaseType, haloSize);

			return new MPIParam(type, idx, size);
		}

		class MPIParam {
			Datatype type;
			int idx, size[];

			public MPIParam(Datatype type, int idx, int[] size) {
				this.type = type;
				this.idx = idx;
				this.size = size;
			}
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

		System.out.println("PID " + p.pid + " data: ");
		int w = p.getPartition().getSize()[0] + 2 * hf.aoi[0];
		int h = p.getPartition().getSize()[1] + 2 * hf.aoi[1];
		for (int i = 0; i < w; i++) {
			if (i == 1 || i == w - 1)
				System.out.println("");
			for (int j = 0; j < h; j++) {
				if (j == 1 || j == h - 1)
					System.out.printf("   \t");
				System.out.printf("%.1f\t", hf.field[i * h + j]);
			}
			System.out.printf("\n");
		}
		System.out.printf("\n");
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);

		MPI.COMM_WORLD.barrier();
	}
}

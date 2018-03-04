package sim.field;

import java.util.Arrays;
import java.util.stream.IntStream;

import mpi.*;
import static mpi.MPI.slice;

import sim.util.IntPoint;
import sim.util.IntHyperRect;

public class HaloField {
	int nd, numNeighbors, maxSendSize;
	int[] fieldSize, haloSize, aoi;
	IntHyperRect origPart, haloPart, privPart;
	Neighbor[] neighbors;
	Comm comm;

	Datatype MPIBaseType = MPI.DOUBLE;

	public HaloField(DPartition ps, int[] aoiv) {
		if (ps.isToroidal())
			throw new UnsupportedOperationException("Toroidal is not supported yet!");

		nd = ps.getNumDim();
		aoi = aoiv;
		comm = ps.getCommunicator();
		fieldSize = ps.getFieldSize();
		origPart = ps.getPartition();

		// Get the partition representing halo and local area by expanding the original partition by aoi at each dimension
		haloPart = origPart.resize(aoi);
		haloSize = haloPart.getSize();

		// Get the partition representing private area by shrinking the original partition by aoi at each dimension
		privPart = origPart.resize(Arrays.stream(aoi).map(x -> -x).toArray());

		// Get the neighbors and create Neighbor objects
		neighbors = Arrays.stream(ps.getNeighborIds())
		            .mapToObj(x -> new Neighbor(ps.getPartition(x)))
		            .toArray(size -> new Neighbor[size]);
		numNeighbors = neighbors.length;

		// Get the max size for one exchange, which is the area of the halo area
		try {
			maxSendSize = comm.packSize(haloPart.getArea() - origPart.getArea(), MPI.DOUBLE);
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}
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

	// Get the corresponding index in a flatted 1-d array of the given point
	public int getFlatIdx(IntPoint p) {
		return IntStream.range(0, nd).map(i -> p.c[i] * stride(i)).sum();
	}

	private int stride(int dim) {
		return IntStream.range(1, nd - dim).reduce(1, (x, i) -> x * haloSize[i]);
	}

	public IntPoint toLocalPoint(IntPoint p) {
		return p.rshift(origPart.ul.c).shift(aoi);
	}

	public void sync(double[] field) throws MPIException {
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
	}

	// Helper class to organize neighbor-related data structures and methods
	class Neighbor {
		int pid;
		MPIParam sendParam, recvParam;

		public Neighbor(IntHyperRect neighborPart) {
			pid = neighborPart.id;
			sendParam = generateMPIParam(origPart.getIntersection(neighborPart.resize(aoi)));
			recvParam = generateMPIParam(haloPart.getIntersection(neighborPart));
		}

		private MPIParam generateMPIParam(IntHyperRect overlap) {
			int[] size = overlap.getSize();
			int idx = getFlatIdx(toLocalPoint(overlap.ul));

			Datatype type = null, base = MPIBaseType;
			try {
				int sizeByte = MPI.COMM_WORLD.packSize(1, base);
				for (int i = nd - 1; i >= 0; i--) {
					type = Datatype.createContiguous(size[i], base);
					type = Datatype.createResized(type, 0, haloSize[i] * sizeByte);
					base = type;
				}
				type.commit();
			} catch (MPIException e) {
				e.printStackTrace();
				System.exit(-1);
			}

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

		DNonUniformPartition p = new DNonUniformPartition(size);

		assert p.np == 4;

		p.insertPartition(new IntHyperRect(0, new IntPoint(new int[] {0, 0}), new IntPoint(new int[] {5, 7})));
		p.insertPartition(new IntHyperRect(1, new IntPoint(new int[] {0, 7}), new IntPoint(new int[] {5, 10})));
		p.insertPartition(new IntHyperRect(2, new IntPoint(new int[] {5, 0}), new IntPoint(new int[] {10, 4})));
		p.insertPartition(new IntHyperRect(3, new IntPoint(new int[] {5, 4}), new IntPoint(new int[] {10, 10})));
		p.setMPITopo();

		HaloField hf = new HaloField(p, aoi);

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

		double[] field = new double[hf.haloPart.getArea()];
		Arrays.fill(field, (double)p.pid);
		hf.sync(field);

		// Try to print in order
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		print2dArray(field,
		             p.getPartition().getSize()[0] + 2 * aoi[0],
		             p.getPartition().getSize()[1] + 2 * aoi[1]
		            );
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);

		MPI.Finalize();
	}

	public static void print2dArray(double[] a, int w, int h) {
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", a[i * h + j]);
			System.out.printf("\n");
		}
	}
}
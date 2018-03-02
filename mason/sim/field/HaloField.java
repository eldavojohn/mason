package sim.field;

import java.util.Arrays;
import java.util.stream.IntStream;
import mpi.*;
import static mpi.MPI.slice;

import sim.util.IntPoint;
import sim.util.IntHyperRect;

public class HaloField {

	int nd, num_neighbors, sendSize;
	int[] fieldSize, partSize, haloSize, aoi;

	boolean isToroidal, isExtendedNeighborhood;

	IntHyperRect partition, haloPart, privatePart;

	Neighbor[][] neighbors;

	Comm comm;

	public HaloField(DPartition ps, int[] aoi) {
		nd = ps.getNumDim();
		this.aoi = aoi;
		comm = ps.getCommunicator();
		fieldSize = ps.getFieldSize();
		partition = ps.getPartition();
		isToroidal = ps.isToroidal();
		isExtendedNeighborhood = ps.isExtendedNeighborhood();

		haloPart = partition.resize(aoi);
		privatePart = partition.resize(Arrays.stream(aoi).map(x -> -x).toArray());
		partSize = partition.getSize();
		haloSize = haloPart.getSize();

		try {
			sendSize = comm.packSize(2 * IntStream.range(0, nd).map(i -> partSize[i] * aoi[i]).sum(), MPI.DOUBLE);
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		int[][] neighbor_pids = ps.getNeighborIdsInOrder();
		num_neighbors = (int)Arrays.stream(neighbor_pids).flatMapToInt(Arrays::stream).count();
		neighbors = new Neighbor[nd * 2][];
		for (int i = 0; i < nd * 2; i++) {
			final int curr_dim = i;
			neighbors[i] = Arrays.stream(neighbor_pids[i])
			            .mapToObj(id -> new Neighbor(ps.getPartition(id), curr_dim))
			            .toArray(size -> new Neighbor[size]);
		}
	}

	public boolean inGlobal(IntPoint p) {
		return IntStream.range(0, nd).allMatch(i -> p.c[i] >= 0 && p.c[i] < fieldSize[i]);
	}

	public boolean inLocal(IntPoint p) {
		return partition.contains(p);
	}

	public boolean inPrivate(IntPoint p) {
		return privatePart.contains(p);
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

	public int getFlatIdx(IntPoint p) {
		return IntStream.range(0, nd).map(i -> p.c[i] * stride(i)).sum();
	}

	private int stride(int dim) {
		return IntStream.range(1, nd - dim).reduce(1, (x, i) -> x * haloSize[i]);
	}

	public void sync(double[] field) throws MPIException {
		byte[] sendbuf = new byte[sendSize];
		byte[] recvbuf = new byte[sendSize];
		int[] pos = new int[num_neighbors];
		int[] count = new int[num_neighbors];
		int lastPos = 0;
		int curr_cnt = 0;

		// Pack
		for (Neighbor[] ds : neighbors)
			for (Neighbor d : ds) {
				pos[curr_cnt] = lastPos;
				lastPos = comm.pack(slice(field, d.packIdx), 1, d.type, sendbuf, lastPos);
				count[curr_cnt] = lastPos - pos[curr_cnt];
				curr_cnt++;
			}

		// Exchange data with neighbors
		// TODO switch to neighborAlltoAllw (so no need to pack/unpack) once it is implemented in OpenMPI Java bindings
		comm.neighborAllToAllv(sendbuf, count, pos, MPI.BYTE, recvbuf, count, pos, MPI.BYTE);

		// Unpack
		curr_cnt = 0;
		for (Neighbor[] ds : neighbors)
			for (Neighbor d : ds)
				comm.unpack(recvbuf, pos[curr_cnt++], slice(field, d.unpackIdx), 1, d.type);
	}

	class Neighbor {

		Datatype type;
		int packIdx, unpackIdx;
		int[] osize;
		int pid;

		public Neighbor(IntHyperRect neighborPart, int d) {
			int dim = d / 2;
			int dir = d % 2;
			int pid = neighborPart.id;

			IntHyperRect overlap = partition.resize(dim, dir, aoi[dim]).intersect(neighborPart);

			// Convert the coordinates to local ones (considering aoi already)
			// TODO
			for (int i = 0; i < nd; i++) {
				overlap.ul.c[i] = overlap.ul.c[i] - partition.ul.c[i] + aoi[i];
				overlap.br.c[i] = overlap.br.c[i] - partition.ul.c[i] + aoi[i];
			}

			osize = overlap.getSize();
			unpackIdx = getFlatIdx(overlap.ul);
			packIdx = getFlatIdx(overlap.ul.shift(dim, dir == 0 ? aoi[dim] : -aoi[dim]));

			Datatype base = MPI.DOUBLE;
			try {
				int sizeByte = MPI.COMM_WORLD.packSize(1, base);
				for (int i = nd - 1; i >= 0; i--) {
					type = Datatype.createContiguous(osize[i], base);
					type = Datatype.createResized(type, 0, haloSize[i] * sizeByte);
					base = type;
				}
				type.commit();
			} catch (MPIException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}

	// public boolean inLocalAndHalo(IntPoint p) {
	// 	boolean ret = true;
	// 	for (int i = 0; i < c.length; i++) {
	// 		int lc = toLocalCoord(c[i], i);
	// 		ret &= lc >= 0 && lc < lsize[i] + 2 * aoi;
	// 	}
	// 	return ret;
	// }

	// public int[] toToroidal(int[] c) {
	// 	for (int i = 0; i < c.length; i++) {
	// 		if (c[i] < 0)
	// 			c[i] += gsize[i];
	// 		else if (c[i] >= gsize[i])
	// 			c[i] -= gsize[i];
	// 	}
	// 	return c;
	// }

	// public int[] toLocalCoords(int[] c) {
	// 	for (int i = 0; i < c.length; i++)
	// 		c[i] = toLocalCoord(c[i], i);
	// 	return c;
	// }

	// private int toLocalCoord(int c, int i) {
	// 	int lc = c - lb[i] + aoi;
	// 	if (lc < 0)
	// 		lc += gsize[i];
	// 	else if (lc >= lsize[i] + 2 * aoi)
	// 		lc -= gsize[i];
	// 	return lc;
	// }

	public static void main(String args[]) throws MPIException {
		int[] size = new int[] {100, 200};
		int[] aoi = new int[] {10, 10};
		int[] want, got;

		MPI.Init(args);

		//DUniformPartition p = new DUniformPartition(size);
		DNonUniformPartition p = new DNonUniformPartition(size);
		p.initUniformly();
		p.setMPITopo();
		HaloField hf = new HaloField(p, aoi);

		//assert p.np == 4;

		assert hf.inGlobal(new IntPoint(new int[] {59, 82}));
		assert !hf.inGlobal(new IntPoint(new int[] { -3, 40}));
		assert !hf.inGlobal(new IntPoint(new int[] {35, 240}));

		// want = new int[] {70, 50};
		// got = hf.toToroidal(new int[] { -30, 250});
		// assert Arrays.equals(want, got);

		// want = new int[] {20, 180};
		// got = hf.toToroidal(new int[] {120, -20});
		// assert Arrays.equals(want, got);

		if (p.pid == 0) {
			assert hf.inLocal(new IntPoint(new int[] {0, 99}));
			assert !hf.inLocal(new IntPoint(new int[] {50, 0}));

			assert hf.inPrivate(new IntPoint(new int[] {aoi[0], aoi[1]}));
			assert !hf.inPrivate(new IntPoint(new int[] {50, 0}));
			assert !hf.inPrivate(new IntPoint(new int[] {0, 99}));

			assert hf.inShared(new IntPoint(new int[] {49, 99}));
			assert !hf.inShared(new IntPoint(new int[] {50, 100}));
			assert !hf.inShared(new IntPoint(new int[] {25, 50}));

			assert hf.inHalo(new IntPoint(new int[] {0, 100}));
			// Commented out due to toroidal
			//assert hf.inHalo(new IntPoint(new int[] {0, 199}));
			//assert hf.inHalo(new IntPoint(new int[] {95, 100}));
			assert !hf.inHalo(new IntPoint(new int[] {25, 50}));
			assert !hf.inHalo(new IntPoint(new int[] {75, 50}));

			// want = new int[] {30, 60};
			// got = hf.toLocalCoords(new int[] {20, 50});
			// assert Arrays.equals(want, got);

			// want = new int[] {5, 5};
			// got = hf.toLocalCoords(new int[] {95, 195});
			// assert Arrays.equals(want, got);
		}

		MPI.Finalize();
	}
}
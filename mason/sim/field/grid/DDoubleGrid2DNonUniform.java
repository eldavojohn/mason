package sim.field.grid;

import java.util.*;
import java.util.stream.*;
import java.nio.DoubleBuffer;

import sim.util.*;
import sim.field.DNonUniformPartition;
import sim.field.HaloFieldGrid;

import java.util.concurrent.TimeUnit;

import mpi.*;
import static mpi.MPI.slice;

public class DDoubleGrid2DNonUniform extends DoubleGrid2D {

	public double[] field;
	public int width, height, pw, ph, psize, aoi;
	public DNonUniformPartition ps;

	Borders b;
	HaloFieldGrid hf;

	// TODO currently no toridal supported / only 5-cell stencil supported
	public DDoubleGrid2DNonUniform(int aoi, double initialValue, DNonUniformPartition ps) {
		super(ps.size[0], ps.size[1]);

		// Init local storage
		IntHyperRect myPart = ps.getPartition();
		psize = ((myPart.br.c[0] - myPart.ul.c[0]) + 2 * aoi) *
		        ((myPart.br.c[1] - myPart.ul.c[1]) + 2 * aoi);
		field = new double[psize];
		for (int i = 0; i < psize; i++)
			field[i] = initialValue;

		this.aoi = aoi;
		this.width = ps.size[0];
		this.height = ps.size[1];
		pw = myPart.br.c[0] - myPart.ul.c[0];
		ph = myPart.br.c[1] - myPart.ul.c[1];

		hf = new HaloFieldGrid(ps, aoi);
		b = new Borders(ps, aoi, hf);
	}

	public final double get(final int x, final int y) {
		int tx = stx(x), ty = sty(y);

		// TODO temporary workaround
		int[] gp = new int[]{tx, ty};
		int[] lp = hf.toLocalCoords(gp);

		// In global
		if (!hf.inGlobal(gp))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of global boundary", ps.pid, Arrays.toString(gp)));

		// In this partition and its surrounding ghost cells
		if (!hf.inLocalAndHalo(lp))
			throw new IllegalArgumentException(String.format("PID %d get %s -> %s is out of local boundary", ps.pid, Arrays.toString(gp), Arrays.toString(lp)));

		return field[b.getIdx(new IntPoint(lp))];
	}

	public final void set(final int x, final int y, final double val) {
		int tx = stx(x), ty = sty(y);

		// TODO temporary workaround
		int[] gp = new int[]{tx, ty};
		int[] lp = hf.toLocalCoords(gp);

		// In global
		if (!hf.inGlobal(gp))
			throw new IllegalArgumentException(String.format("PID %d set %s is out of global boundary", ps.pid, Arrays.toString(gp)));

		// In this partition but not in ghost cells
		if (!hf.inLocal(lp))
			throw new IllegalArgumentException(String.format("PID %d set %s -> %s is out of global boundary", ps.pid, Arrays.toString(gp), Arrays.toString(lp)));

		field[b.getIdx(new IntPoint(lp))] = val;
	}

	public void sync() throws MPIException {
		b.sync(field);
	}

	// // TODO
	// public double[] collect(int dst) throws MPIException {
		
	// }

	public static void print2dArray(double[] a, int w, int h) {
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", a[i * h + j]);
			System.out.printf("\n");
		}
	}

	public static void main(String[] args) throws MPIException, InterruptedException {
		MPI.Init(args);

		DNonUniformPartition p = new DNonUniformPartition(new int[] {8, 8});

		assert p.np == 4;

		// IntHyperRect r1 = new IntHyperRect(0, new IntPoint(new int[]{0, 0}), new IntPoint(new int[]{4, 8}));
		// IntHyperRect r2 = new IntHyperRect(1, new IntPoint(new int[]{4, 0}), new IntPoint(new int[]{8, 8}));
		// IntHyperRect r1 = new IntHyperRect(0, new IntPoint(new int[]{0, 0}), new IntPoint(new int[]{8, 4}));
		// IntHyperRect r2 = new IntHyperRect(1, new IntPoint(new int[]{0, 4}), new IntPoint(new int[]{8, 8}));
		// p.insertPartition(r1);
		// p.insertPartition(r2);

		p.initUniformly();
		p.setMPITopo();

		//TimeUnit.SECONDS.sleep(p.pid);
		//System.out.println("PID " + p.pid + " " + p.getPartition());
		//TimeUnit.SECONDS.sleep(p.np - p.pid);

		//TimeUnit.SECONDS.sleep(p.pid);
		DDoubleGrid2DNonUniform f = new DDoubleGrid2DNonUniform(2, p.pid, p);
		//TimeUnit.SECONDS.sleep(p.np - p.pid);

		// f.field[7] = p.pid + 1;
		// f.field[10] = p.pid + 1;
		// f.field[25] = p.pid + 1;
		// f.field[28] = p.pid + 1;

		//TimeUnit.SECONDS.sleep(p.pid);
		f.sync();
		//TimeUnit.SECONDS.sleep(p.np - p.pid);

		TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		print2dArray(f.field, 8, 8);
		TimeUnit.SECONDS.sleep(p.np - p.pid);

		MPI.Finalize();
	}
}

// TODO consider merge this class with HaloField;
class Borders {
	int nd, aoi, num_neighbors;
	int[][] pids;
	int[] lsize;

	Delimiter[][] delims;
	IntHyperRect myPart;

	HaloFieldGrid hf;

	GraphComm comm;
	DNonUniformPartition p;

	public Borders(DNonUniformPartition p, int aoi, HaloFieldGrid hf) {
		this.nd = p.nd;
		this.aoi = aoi;
		this.comm = p.comm;
		this.hf = hf;
		this.p = p;

		myPart = p.getPartition();
		lsize = IntStream.range(0, nd).map(i -> myPart.br.c[i] - myPart.ul.c[i]).toArray();

		delims = new Delimiter[nd * 2][];
		pids = p.getNeighborIdsInOrder();
		num_neighbors = (int)Arrays.stream(pids).flatMapToInt(Arrays::stream).count();

		for (int i = 0; i < nd * 2; i++) {
			final int curr_dim = i;
			delims[i] = Arrays.stream(pids[i])
			            .mapToObj(x -> new Delimiter(p.getPartition(x), curr_dim))
			            .toArray(size -> new Delimiter[size]);
		}
	}

	public int getIdx(IntPoint p) {
		return IntStream.range(0, nd).map(i -> p.c[i] * stride(i)).sum();
	}

	public int stride(int dim) {
		int stride = 1;

		for (int i = 1; i < nd - dim; i++)
			stride *= lsize[i] + 2 * aoi;

		return stride;
	}

	public void sync(double[] field) throws MPIException {
		int sendsize = comm.packSize(2 * aoi * Arrays.stream(lsize).sum(), MPI.DOUBLE);
		byte[] sendbuf = new byte[sendsize];
		byte[] recvbuf = new byte[sendsize];
		int[] pos = new int[num_neighbors], count = new int[num_neighbors];
		int lastPos = 0;
		int curr_cnt = 0;

		// Pack
		for (Delimiter[] ds : delims)
			for (Delimiter d : ds) {
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
		for (Delimiter[] ds : delims)
			for (Delimiter d : ds)
				comm.unpack(recvbuf, pos[curr_cnt++], slice(field, d.unpackIdx), 1, d.type);
	}

	class Delimiter {

		Datatype type;
		int packIdx, unpackIdx;
		int[] osize;

		public Delimiter(IntHyperRect neighborPart, int d) {
			int dim = d / 2, dir = d % 2;

			// TODO must be a better way of doing this.
			// *************
			if (dir == 0) 	// backward direction
				myPart.ul.c[dim] -= aoi;
			else				// forward direction
				myPart.br.c[dim] += aoi;

			IntHyperRect overlap = myPart.intersect(neighborPart);

			if (dir == 0) 	// backward direction
				myPart.ul.c[dim] += aoi;
			else				// forward direction
				myPart.br.c[dim] -= aoi;
			// *************

			// Convert the coordinates to local ones (considering aoi already)
			// TODO
			for (int i = 0; i < nd; i++) {
				overlap.ul.c[i] = overlap.ul.c[i] - myPart.ul.c[i] + aoi;
				overlap.br.c[i] = overlap.br.c[i] - myPart.ul.c[i] + aoi;
			}

			osize = IntStream.range(0, nd).map(i -> overlap.br.c[i] - overlap.ul.c[i]).toArray();
			unpackIdx = getIdx(overlap.ul);
			overlap.ul.shift(dim, dir == 0 ? aoi : -aoi);
			packIdx = getIdx(overlap.ul);

			Datatype base = MPI.DOUBLE;
			try {
				int sizeByte = MPI.COMM_WORLD.packSize(1, base);
				for (int i = nd - 1; i >= 0; i--) {
					type = Datatype.createContiguous(osize[i], base);
					type = Datatype.createResized(type, 0, (lsize[i] + 2 * aoi) * sizeByte);
					base = type;
				}
				type.commit();
			} catch (MPIException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
	}
}
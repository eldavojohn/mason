package sim.field.grid;

import java.util.*;
import java.util.stream.*;

import sim.util.*;
import sim.field.DNonUniformPartition;
import sim.field.HaloField;

import java.util.concurrent.TimeUnit;

import mpi.*;
import static mpi.MPI.slice;

public class DDoubleGrid2DNonUniform extends DoubleGrid2D {

	public double[] field;
	public DNonUniformPartition ps;
	public HaloField hf;

	// TODO currently no toridal supported
	public DDoubleGrid2DNonUniform(int[] aoi, double initialValue, DNonUniformPartition ps) {
		super(ps.size[0], ps.size[1]);

		// Init local storage
		int[] psize = ps.getPartition().getSize();
		int bufsize = IntStream.range(0, psize.length).map(i -> psize[i] + 2 * aoi[i]).reduce(1, (x, y) -> x * y);
		field = new double[bufsize];
		Arrays.fill(field, initialValue);

		hf = new HaloField(ps, aoi);
	}

	public final double get(final int x, final int y) {
		IntPoint p = new IntPoint(new int[]{stx(x), sty(y)});

		// In global
		if (!hf.inGlobal(p))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of global boundary", ps.pid, p.toString()));

		// In this partition and its surrounding ghost cells
		if (!hf.inLocalAndHalo(p))
			throw new IllegalArgumentException(String.format("PID %d get %s is out of local boundary", ps.pid, p.toString()));

		return field[hf.getFlatIdx(hf.toLocalPoint(p))];
	}

	public final void set(final int x, final int y, final double val) {
		IntPoint p = new IntPoint(new int[]{stx(x), sty(y)});

		// In global
		if (!hf.inGlobal(p))
			throw new IllegalArgumentException(String.format("PID %d set %s is out of global boundary", ps.pid, p.toString()));

		// In this partition but not in ghost cells
		if (!hf.inLocal(p))
			throw new IllegalArgumentException(String.format("PID %d set %s is out of local boundary", ps.pid, p.toString()));

		field[hf.getFlatIdx(hf.toLocalPoint(p))] = val;
	}

	public void sync() throws MPIException {
		hf.sync(field);
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
		p.initUniformly();
		p.setMPITopo();
		DDoubleGrid2DNonUniform f = new DDoubleGrid2DNonUniform(new int[]{2, 2}, p.pid, p);
		f.sync();

		TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		print2dArray(f.field, 8, 8);
		TimeUnit.SECONDS.sleep(p.np - p.pid);

		MPI.Finalize();
	}
}
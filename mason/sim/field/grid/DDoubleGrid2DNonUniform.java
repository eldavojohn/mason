package sim.field.grid;

import java.io.IOException;
import java.util.*;
import java.util.stream.*;

import sim.util.*;
import sim.field.DPartition;
import sim.field.DNonUniformPartition;
import sim.field.HaloField;

import java.util.concurrent.TimeUnit;

import mpi.*;
import static mpi.MPI.slice;

// Adatper class between HaloField and the original DoubleGrid2D
public class DDoubleGrid2DNonUniform extends DoubleGrid2D {
	public HaloField hf;

	public DDoubleGrid2DNonUniform(int[] aoi, double initialValue, DPartition ps) {
		super(ps.getFieldSize()[0], ps.getFieldSize()[1]);
		hf = new HaloField(ps, aoi, initialValue);
	}

	public final double get(final int x, final int y) {
		IntPoint p = new IntPoint(new int[] {stx(x), sty(y)});

		return hf.get(p);
	}

	public final void set(final int x, final int y, final double val) {
		IntPoint p = new IntPoint(new int[] {stx(x), sty(y)});

		hf.set(p, val);
	}

	public void sync() throws MPIException, IOException {
		hf.sync();
	}

	public double[] collect(int dst) throws MPIException, IOException {
		return hf.collect(dst);
	}

	public static void print2dArray(double[] a, int w, int h) {
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", a[i * h + j]);
			System.out.printf("\n");
		}
	}

	public static void main(String[] args) throws MPIException, InterruptedException, IOException {
		MPI.Init(args);

		int[] aoi = new int[] {2, 2};
		int[] size = new int[] {8, 8};

		DNonUniformPartition p = new DNonUniformPartition(size);
		p.initUniformly(null);
		p.setMPITopo();
		DDoubleGrid2DNonUniform f = new DDoubleGrid2DNonUniform(aoi, p.getPid(), p);

		f.sync();

		// Try to print in order in MPI environment
		TimeUnit.SECONDS.sleep(p.getPid());

		System.out.println("PID " + p.getPid() + " data: ");
		print2dArray(f.hf.getField(), 8, 8);

		TimeUnit.SECONDS.sleep(p.getNumProc() - p.getPid());

		MPI.COMM_WORLD.barrier();

		double[] fullField = f.collect(0);
		if (p.getPid() == 0) {
			System.out.println("Full Field: ");
			print2dArray(fullField, 8, 8);
		}

		MPI.Finalize();
	}
}

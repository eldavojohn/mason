package sim.field.grid;

import java.util.*;
import java.util.stream.*;
import java.nio.DoubleBuffer;

import sim.util.*;
import sim.field.DNonUniformPartition;
import sim.field.Partition;

import mpi.*;
import static mpi.MPI.slice;

public class DDoubleGrid2DNonUniform extends DoubleGrid2D {

	public double[] field;
	public int width, height, pw, ph, psize, aoi;
	public DNonUniformPartition ps;

	CartComm comm;
	Datatype wtype, htype, ctype, ptype, p2type;

	public DDoubleGrid2DNonUniform(int width, int height, int aoi, double initialValue, DNonUniformPartition ps) {
		super(ps.size[0], ps.size[1]);

		// // Init local storage
		// Partition myPart = ps.getPartition();
		// // TODO(hw) fix the type in Partition
		// psize = ((int)(myPart.br[0] - myPart.ul[0]) + 2 * aoi) *
		//         ((int)(myPart.br[1] - myPart.ul[1]) + 2 * aoi);
		// field = new double[psize];
		// for (int i = 0; i < psize; i++)
		// 	field[i] = initialValue;

		// // Setup borders
		// int[][] border_delims = new int[ps.nd * 2][];

	}
}

class Borders {
	int nd, aoi;
	int[][] pids;
	IntHyperRect[][] delims;

	Datatype[] dt;

	public Borders(DNonUniformPartition p, int aoi) {
		this.nd = p.nd;
		this.aoi = aoi;

		delims = new IntHyperRect[nd * 2][];
		pids = p.getNeighborIdsInOrder();

		for (int i = 0, curr = 0; i < nd * 2; i++) {
			final int curr_dim = i;
			// TODO need to reduce dim?
			delims[i] = Arrays.stream(pids[i])
			            .mapToObj(x -> p.getPartition(x).reduceDim(curr_dim))
			            .toArray(size -> new IntHyperRect[size]);
		}

		// TODO Generat MPI packing/unpacking data types

	}

	private Datatype initMPIDataType(int dim) {
		try {
			Datatype dt = Datatype.createVector(, , , MPI.DOUBLE);
			dt.commit();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		return dt;
	}
}
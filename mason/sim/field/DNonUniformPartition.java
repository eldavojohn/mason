package sim.field;

import java.util.*;

import sim.util.*;

import mpi.*;

public class DNonUniformPartition extends DPartition {

	public AugmentedSegmentTree st[];
	// TODO Use IntHyperRect for now, need to use something like generic or create separate files for int and double.
	public Map<Integer, IntHyperRect> ps;

	public final double epsilon = 0.0001;

	public DNonUniformPartition(int size[]) {
		this(size, false);
	}

	public DNonUniformPartition(int size[], boolean isToroidal) {
		super(size, isToroidal);

		this.st = new AugmentedSegmentTree[nd];
		for (int i = 0; i < nd; i++)
			this.st[i] = new AugmentedSegmentTree(isToroidal);

		ps = new HashMap<Integer, IntHyperRect>();

		try {
			pid = MPI.COMM_WORLD.getRank();
			np = MPI.COMM_WORLD.getSize();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void setMPITopo() {
		// TODO Currently a LP holds one partition. need to add support for cases that a LP holds multiple partitions
		if (ps.size() != np)
			throw new IllegalArgumentException(String.format("The number of partitions (%d) must equal to the number of LPs (%d)", ps.size(), np));

		// Get sorted neighbor ids list
		int[] ns = Arrays.stream(getNeighborIds())
		           .mapToObj(x -> ps.get(x)).sorted()
		           .mapToInt(x -> x.id).toArray();

		// Create a unweighted & undirected graph
		try {
			comm = MPI.COMM_WORLD.createDistGraphAdjacent(
			           ns,
			           ns,
			           new Info(),
			           false
			       );
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		// TODO: re-map LPs to Partitions for optimal LP placement
	}

	public void initUniformly() {
		int[] dims = new int[nd], psize = new int[nd], coord = new int[nd];

		// Generate a nd mesh of np processors
		try {
			CartComm.createDims(np, dims);
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		for (int i = 0; i < nd; i++)
			psize[i] = size[i] / dims[i];

		initUniformlyRecursive(coord, 0, dims, psize);
	}

	private void initUniformlyRecursive(int[] coord, int curr, int[] dims, int psize[]) {
		if (curr == nd) {
			int[] ul = new int[nd], br = new int[nd];
			for (int i = 0; i < nd; i++) {
				ul[i] = coord[i] * psize[i];
				br[i] = ul[i] + psize[i];
			}
			insertPartition(new IntHyperRect(
			                    ps.size(),
			                    new IntPoint(ul),
			                    new IntPoint(br)
			                ));
		} else
			for (int i = 0; i < dims[curr]; i++) {
				coord[curr] = i;
				initUniformlyRecursive(coord, curr + 1, dims, psize);
			}
	}

	// Insert a partition into the DNonUniformPartition scheme
	public void insertPartition(IntHyperRect p) {
		if (ps.containsKey(p.id))
			throw new IllegalArgumentException("The partition id to be inserted already exists");

		for (int i = 0; i < nd; i++)
			st[i].insert(p.getSegment(i));

		ps.put(p.id, p);
	}

	public void removePartition(final int pid) {
		// TODO
	}

	public void updatePartition(final double[] ul, final double[] br, final int pid) {
		// TODO
	}

	public IntHyperRect getPartition() {
		return getPartition(this.pid);
	}

	public IntHyperRect getPartition(int pid) {
		IntHyperRect rect = ps.get(pid);

		if (rect == null)
			throw new IllegalArgumentException("PID " + pid + " has no corresponding partition");

		return rect;
	}

	// Stabbing query
	public int toPartitionId(final int[] c) {
		return toPartitionId(Arrays.stream(c).mapToDouble(x -> (double)x).toArray());
	}

	public int toPartitionId(IntPoint p) {
		return toPartitionId(p.c);
	}

	public int toPartitionId(final double[] c) {
		Set<Integer> ret = st[0].toPartitions(c[0]);

		for (int i = 1; i < nd; i++)
			ret.retainAll(st[i].toPartitions(c[i]));

		if (ret.size() != 1)
			throw new IllegalArgumentException("Point " + Arrays.toString(c) + " belongs to multiple pids or no pid: " + ret);

		return ret.toArray(new Integer[0])[0];
	}

	// Range query
	public Set<Integer> coveredPartitionIds(final int[] ul, final int[] br) {
		return coveredPartitionIds(Arrays.stream(ul).mapToDouble(x -> (double)x).toArray(),
		                           Arrays.stream(br).mapToDouble(x -> (double)x).toArray());
	}

	public Set<Integer> coveredPartitionIds(final double[] ul, final double[] br) {
		Set<Integer> ret = st[0].toPartitions(ul[0], br[0]);

		for (int i = 1; i < nd; i++)
			ret.retainAll(st[i].toPartitions(ul[i], br[i]));

		if (ret.size() < 1)
			throw new IllegalArgumentException("Rectangle <" + Arrays.toString(ul) + ", " + Arrays.toString(br) + "> covers no pid: " + ret);

		return ret;
	}

	public int[] getNeighborIds() {
		IntHyperRect rect = getPartition();

		// TODO Better way?
		// Expanded all dimensions by epsilon
		double[] exp_ul = Arrays.stream(rect.ul.c).mapToDouble(x -> (double)x - epsilon).toArray();
		double[] exp_br = Arrays.stream(rect.br.c).mapToDouble(x -> (double)x + epsilon).toArray();

		// Remove self
		return coveredPartitionIds(exp_ul, exp_br).stream()
		       .filter(i -> i != pid).mapToInt(i -> i).toArray();
	}

	// Get neighbor ids on each dimension (backward first, then forward)
	public int[][] getNeighborIdsInOrder() {
		int[][] ret = new int[nd * 2][];

		for (int i = 0; i < nd * 2; i++) {
			final int curr_dim = i / 2, dir = i % 2 - 1;
			ret[i] = Arrays.stream(getNeighborIdsShift(curr_dim, dir))
			         .mapToObj(x -> ps.get(x).reduceDim(curr_dim)).sorted()
			         .mapToInt(x -> x.id).toArray();
		}

		return ret;
	}

	// Get the neighbor id specified by dimension and direction (forward >=0 / backward < 0)
	public int[] getNeighborIdsShift(int dim, int dir) {
		IntHyperRect rect = getPartition();

		double[] exp_ul = Arrays.stream(rect.ul.c).mapToDouble(x -> (double)x).toArray();
		double[] exp_br = Arrays.stream(rect.br.c).mapToDouble(x -> (double)x).toArray();

		if (dir >= 0)
			exp_br[dim] += epsilon;
		else
			exp_ul[dim] -= epsilon;

		return coveredPartitionIds(exp_ul, exp_br).stream()
		       .filter(i -> i != pid).mapToInt(i -> i).toArray();
	}

	public static void main(String args[]) throws MPIException, InterruptedException {
		MPI.Init(args);

		testNonUniform();

		testNonUniformToroidal();

		// // Second test for initUniformly()
		// DNonUniformPartition p2 = new DNonUniformPartition(new int[] {12, 24});
		// assert np == 12;

		// p2.initUniformly();

		// System.out.println("PID " + p2.pid + " Neighbors: " + Arrays.toString(p2.getNeighborIds()));

		MPI.Finalize();
	}

	public static void testNonUniformToroidal() throws MPIException, InterruptedException {
		DNonUniformPartition p = new DNonUniformPartition(new int[] {10, 20}, true);
		assert p.np == 5;

		/**
		* Create the following partition scheme
		*
		*	 0		8		12			20
		*	0 ---------------------------
		*	  |		0		|			|
		*	3 |-------------|	  1		|
		*	  |		|	4	|			|
		*	7 |	 3	|-------------------|
		*	  |		|		 2			|
		*  10 ---------------------------
		*
		**/

		p.insertPartition(new IntHyperRect(0, new IntPoint(new int[] {0, 0}), new IntPoint(new int[] {3, 12})));
		p.insertPartition(new IntHyperRect(1, new IntPoint(new int[] {0, 12}), new IntPoint(new int[] {7, 20})));
		p.insertPartition(new IntHyperRect(2, new IntPoint(new int[] {7, 8}), new IntPoint(new int[] {10, 20})));
		p.insertPartition(new IntHyperRect(3, new IntPoint(new int[] {3, 0}), new IntPoint(new int[] {10, 8})));
		p.insertPartition(new IntHyperRect(4, new IntPoint(new int[] {3, 8}), new IntPoint(new int[] {7, 12})));

		double[] c, c1, c2;

		if (p.pid == 0) {

			c = new double[] {0, 0};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c = new double[] { -1, -1};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c = new double[] {14, 21};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c = new double[] {7, 24};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c = new double[] {27, 45};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c1 = new double[] {7, 8};
			c2 = new double[] {11, 20};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));

			c1 = new double[] {0, -1};
			c2 = new double[] {3, 12};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));

			c1 = new double[] { -1, -1};
			c2 = new double[] {4, 13};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));

			c1 = new double[] { -1, -2};
			c2 = new double[] {11, 2};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));

			c1 = new double[] {-1, -2};
			c2 = new double[] {2, 22};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));
		}

		MPI.COMM_WORLD.barrier();

		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " Neighbors: " + Arrays.toString(p.getNeighborIds()));
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);

		MPI.COMM_WORLD.barrier();
	}

	public static void testNonUniform() throws MPIException {
		DNonUniformPartition p = new DNonUniformPartition(new int[] {10, 20});
		assert p.np == 5;

		/**
		* Create the following partition scheme
		*
		*	 0		8		12			20
		*	0 ---------------------------
		*	  |		0		|			|
		*	3 |-------------|	  1		|
		*	  |		|	4	|			|
		*	7 |	 3	|-------------------|
		*	  |		|		 2			|
		*  10 ---------------------------
		*
		**/
		p.insertPartition(new IntHyperRect(0, new IntPoint(new int[] {0, 0}), new IntPoint(new int[] {3, 12})));
		p.insertPartition(new IntHyperRect(1, new IntPoint(new int[] {0, 12}), new IntPoint(new int[] {7, 20})));
		p.insertPartition(new IntHyperRect(2, new IntPoint(new int[] {7, 8}), new IntPoint(new int[] {10, 20})));
		p.insertPartition(new IntHyperRect(3, new IntPoint(new int[] {3, 0}), new IntPoint(new int[] {10, 8})));
		p.insertPartition(new IntHyperRect(4, new IntPoint(new int[] {3, 8}), new IntPoint(new int[] {7, 12})));

		double[] c, c1, c2;

		if (p.pid == 0) {

			c = new double[] {0, 0};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c = new double[] {4, 9};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c = new double[] {4, 12};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c = new double[] {7, 8};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c = new double[] {7, 5};
			System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

			c1 = new double[] {3, 8};
			c2 = new double[] {7, 12};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));

			c1 = new double[] {3, 8};
			c2 = new double[] {8, 12};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));

			c1 = new double[] {3, 8};
			c2 = new double[] {7, 20};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));

			c1 = new double[] {2, 8};
			c2 = new double[] {7, 20};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));

			c1 = new double[] {2, 7};
			c2 = new double[] {8, 20};
			System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
			                   Arrays.toString(p.coveredPartitionIds(c1, c2).toArray()));
		}

		MPI.COMM_WORLD.barrier();

		System.out.println("PID " + p.pid + " Neighbors: " + Arrays.toString(p.getNeighborIds()));
		System.out.println("PID " + p.pid + " Neighbors in order: " + Arrays.toString(Arrays.stream(p.getNeighborIdsInOrder()).flatMapToInt(Arrays::stream).toArray()));

		p.setMPITopo();
		GraphComm gc = (GraphComm)p.comm;
		DistGraphNeighbors nsobj = gc.getDistGraphNeighbors();
		int[] ns = new int[nsobj.getOutDegree()];
		for (int i = 0; i < nsobj.getOutDegree(); i++)
			ns[i] = nsobj.getDestination(i);

		System.out.println("PID " + p.pid + " MPI Neighbors: " + Arrays.toString(ns));

		MPI.COMM_WORLD.barrier();
	}
}

class AugmentedSegmentTree extends SegmentTree {

	public AugmentedSegmentTree(boolean isToroidal) {
		super(isToroidal);
	}

	public Set<Integer> toPartitions(int target) {
		List<Segment> res = contains((double)target);
		Set<Integer> s = new HashSet<Integer>();
		res.forEach(seg -> s.add(seg.pid));

		return s;
	}

	public Set<Integer> toPartitions(double target) {
		List<Segment> res = contains(target);
		Set<Integer> s = new HashSet<Integer>();
		res.forEach(seg -> s.add(seg.pid));

		return s;
	}

	public Set<Integer> toPartitions(int st, int ed) {
		List<Segment> res = intersect((double)st, (double)ed);
		Set<Integer> s = new HashSet<Integer>();
		res.forEach(seg -> s.add(seg.pid));

		return s;
	}

	public Set<Integer> toPartitions(double st, double ed) {
		List<Segment> res = intersect(st, ed);
		Set<Integer> s = new HashSet<Integer>();
		res.forEach(seg -> s.add(seg.pid));

		return s;
	}
}

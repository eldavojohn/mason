package sim.field;

import java.util.*;

import sim.util.*;

import mpi.*;

public class DNonUniformPartition {

	public int size[], nd, np, pid;
	public AugmentedSegmentTree st[];
	public Map<Integer, Partition> ps;
	public GraphComm comm;

	public double epsilon = 0.0001;

	public DNonUniformPartition(int size[]) {
		this.nd = size.length;
		this.size = Arrays.copyOf(size, nd);

		this.st = new AugmentedSegmentTree[this.nd];
		for (int i = 0; i < nd; i++)
			this.st[i] = new AugmentedSegmentTree();

		ps = new HashMap<Integer, Partition>();

		try {
			pid = MPI.COMM_WORLD.getRank();
			np = MPI.COMM_WORLD.getSize();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void setMPITopo() {
		if (ps.size() != np)
			throw new IllegalArgumentException(String.format("The number of partitions (%d) must equal to the number of LPs (%d)", ps.size(), np));

		int[] ns = getNeighborIDs(pid);

		try {
			// Create a unweighted & undirected graph
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
			double[] ul = new double[nd], br = new double[nd];
			for (int i = 0; i < nd; i++) {
				ul[i] = coord[i] * psize[i];
				br[i] = ul[i] + psize[i];
			}
			insertPartition(ul, br, ps.size());
		}
		else
			for (int i = 0; i < dims[curr]; i++) {
				coord[curr] = i;
				initUniformlyRecursive(coord, curr + 1, dims, psize);
			}
	}

	// TODO add another layer of indirection
	// Currently a LP holds one partition -> a LP holds multiple partitions

	// Insert a partition into the DNonUniformPartition scheme
	public void insertPartition(Partition p) {
		if (ps.containsKey(p.pid))
			throw new IllegalArgumentException("The partition id to be inserted already exists");

		for (int i = 0; i < nd; i++)
			st[i].insert(new Segment(p.ul[i], p.br[i], p.pid));

		ps.put(p.pid, p);
	}

	public void insertPartition(final double[] ul, final double[] br, final int pid) {
		insertPartition(new Partition(ul, br, pid));
	}

	public void removePartition(final int pid) {
		//TODO
	}

	public void updatePartition(final double[] ul, final double[] br, final int pid) {
		// TODO
	}

	// Stabbing query
	public int toPartitionId(final double[] c) {
		Set<Integer> ret = st[0].toPartitions(c[0]);

		for (int i = 1; i < nd; i++) {
			ret.retainAll(st[i].toPartitions(c[i]));
		}

		if (ret.size() != 1)
			throw new IllegalArgumentException("Point " + Arrays.toString(c) + " belongs to multiple pids or no pid: " + ret);

		return ret.toArray(new Integer[0])[0];
	}

	// Range query
	public Set<Integer> coveredPartitionIds(final double[] ul, final double[] br) {
		Set<Integer> ret = st[0].toPartitions(ul[0], br[0]);

		for (int i = 1; i < nd; i++) {
			ret.retainAll(st[i].toPartitions(ul[i], br[i]));
		}

		if (ret.size() < 1)
			throw new IllegalArgumentException("Rectangle <" + Arrays.toString(ul) + ", " + Arrays.toString(br) + "> covers no pid: " + ret);

		return ret;
	}

	public int[] getNeighborIDs(int pid) {
		Partition sp = ps.get(pid);

		if (sp == null)
			throw new IllegalArgumentException("Invalid PID " + pid);

		double[] ul = new double[nd], br = new double[nd];

		for (int i = 0; i < nd; i++) {
			ul[i] = sp.ul[i] - epsilon;
			br[i] = sp.br[i] + epsilon;
		}

		Set<Integer> res = coveredPartitionIds(ul, br);

		assert res.contains(pid);

		res.remove(pid);

		return res.stream().mapToInt(i->i).toArray();
	}

	public static void main(String args[]) throws MPIException {
		MPI.Init(args);

		DNonUniformPartition p = new DNonUniformPartition(new int[] {10, 20});
		assert p.np == 5;

		p.insertPartition(new double[] {0, 0}, new double[] {3, 12}, 0);
		p.insertPartition(new double[] {0, 12}, new double[] {7, 20}, 1);
		p.insertPartition(new double[] {7, 8}, new double[] {10, 20}, 2);
		p.insertPartition(new double[] {3, 0}, new double[] {10, 8}, 3);
		p.insertPartition(new double[] {3, 8}, new double[] {7, 12}, 4);

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

		int id = p.pid;
		System.out.println("PID " + id + " Neighbors: " + Arrays.toString(p.getNeighborIDs(id)));

		p.setMPITopo();

		DistGraphNeighbors nsobj = p.comm.getDistGraphNeighbors();
		int[] ns = new int[nsobj.getOutDegree()];
		for (int i = 0; i < nsobj.getOutDegree(); i++)
			ns[i] = nsobj.getDestination(i);

		System.out.println("PID " + id + " MPI Neighbors: " + Arrays.toString(ns));

		// // Second test for initUniformly() 
		// DNonUniformPartition p2 = new DNonUniformPartition(new int[] {12, 24});
		// assert np == 12;

		// p2.initUniformly();

		// int id = p2.pid;
		// System.out.println("PID " + id + " Neighbors: " + Arrays.toString(p2.getNeighborIDs(id)));

		MPI.Finalize();
	}
}

class AugmentedSegmentTree extends SegmentTree {

	public Set<Integer> toPartitions(double target) {
		List<Segment> res = contains(target);
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

class Partition {
	double[] ul;
	double[] br;
	int pid;

	public Partition(final double[] ul, final double[] br, final int pid) {
		this.ul = ul;
		this.br = br;
		this.pid = pid;
	}
}
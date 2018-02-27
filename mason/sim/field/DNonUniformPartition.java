package sim.field;

import java.util.*;
import java.util.stream.Collectors;

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

		int[] ns = getNeighborIdsInOrder();

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
		} else
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

	public Partition getMyPartition() {
		return ps.get(pid);
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

	public int[] getNeighborIds() {
		return getNeighborIds(this.pid);
	}

	public int[] getNeighborIds(int pid) {
		Partition sp = ps.get(pid);

		if (sp == null)
			throw new IllegalArgumentException("PID " + pid + " has no corresponding partition");

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

	public int[] getNeighborIdsInOrder() {
		List<Integer> ret = new ArrayList<Integer>();

		for (int i = 0; i < nd; i++) 
			for (int dir : new int[] { -1, 1}) {
				List<Integer> ids = Arrays.stream(getNeighborIdsShift(i, dir)).boxed().collect(Collectors.toList());
				final int skip = i;
				ids.sort(new Comparator<Integer>() {
					@Override
					public int compare(Integer self, Integer other) {
						Partition sp = ps.get(self), op = ps.get(other);

						for (int d = 0; d < nd; d++) {
							if (d == skip || sp.ul[d] == op.ul[d])
								continue;
							if (sp.ul[d] < op.ul[d])
								return -1;
							if (sp.ul[d] > op.ul[d])
								return 1;
						}

						return 0;
					}
				});
				ret.addAll(ids);
			}

		return ret.stream().mapToInt(i->i).toArray();
	}

	public int[] getNumNeighbors() {
		int[] ret = new int[nd * 2];

		for (int i = 0; i < nd; i++) {
			ret[i * 2] = getNeighborIdsShift(i, -1).length;
			ret[i * 2 + 1] = getNeighborIdsShift(i, 1).length;
		}

		return ret;
	}

	// Get the neighbor id specified by dimension and direction (forward >=0 / backward < 0)
	public int[] getNeighborIdsShift(int dim, int dir) {
		return getNeighborIdsShift(pid, dim, dir);
	}

	public int[] getNeighborIdsShift(int pid, int dim, int dir) {
		Partition sp = ps.get(pid);

		if (sp == null)
			throw new IllegalArgumentException("PID " + pid + " has no corresponding partition");

		double[] ul = Arrays.copyOf(sp.ul, nd);
		double[] br = Arrays.copyOf(sp.br, nd);

		if (dir >= 0)
			br[dim] += epsilon;
		else
			ul[dim] -= epsilon;

		Set<Integer> res = coveredPartitionIds(ul, br);

		assert res.contains(pid);

		res.remove(pid);

		return res.stream().mapToInt(i->i).toArray();
	}

	// return [dim, dir] representing topid is on the forward/backward direction of dim dimension, relative to frompid.
	// assuming topid is one of the neighbors of frompid
	public int[] getRelativeDirection(int frompid, int topid) {
		Partition fp = ps.get(frompid), tp = ps.get(topid);
		assert fp != null && tp != null;

		for (int i = 0; i < nd; i++)
			if (tp.br[i] <= fp.ul[i]) 		// tp is above fp
				return new int[] {i, -1};
			else if (tp.ul[i] >= fp.br[i]) 	// tp is below fp
				return new int[] {i, 1};

		// Nothing match - return error
		return null;
	}

	public static void main(String args[]) throws MPIException {
		MPI.Init(args);

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

		System.out.println("PID " + p.pid + " Neighbors: " + Arrays.toString(p.getNeighborIds()));
		System.out.println("PID " + p.pid + " Neighbors in order: " + Arrays.toString(p.getNeighborIdsInOrder()));

		p.setMPITopo();

		DistGraphNeighbors nsobj = p.comm.getDistGraphNeighbors();
		int[] ns = new int[nsobj.getOutDegree()];
		for (int i = 0; i < nsobj.getOutDegree(); i++)
			ns[i] = nsobj.getDestination(i);

		System.out.println("PID " + p.pid + " MPI Neighbors: " + Arrays.toString(ns));

		// // Second test for initUniformly()
		// DNonUniformPartition p2 = new DNonUniformPartition(new int[] {12, 24});
		// assert np == 12;

		// p2.initUniformly();

		// int id = p2.pid;
		// System.out.println("PID " + id + " Neighbors: " + Arrays.toString(p2.getNeighborIds(id)));

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
package sim.field;

import java.util.*;

import sim.util.*;

class Partition {
	int[] ul;
	int[] br;
	int pid;

	public Partition(final int[] ul, final int[] br, final int pid) {
		this.ul = ul;
		this.br = br;
		this.pid = pid;
	}
}

public class DNonUniformPartition {

	public int size[], nd, np, pid;
	public AugmentedSegmentTree st[];
	public Set<Partition> ps;

	public DNonUniformPartition(int size[]) {
		this.nd = size.length;
		this.size = Arrays.copyOf(size, nd);

		this.st = new AugmentedSegmentTree[this.nd];
		for (int i = 0; i < nd; i++)
			this.st[i] = new AugmentedSegmentTree();

		ps = new HashSet<Partition>();
	}

	// Insert a partition into the DNonUniformPartition scheme
	public void insertPartition(Partition p) {
		for (int i = 0; i < nd; i++)
			st[i].insert(new Segment(p.ul[i], p.br[i], p.pid));
		ps.add(p);
	}

	public void insertPartition(final int[] ul, final int[] br, final int pid) {
		insertPartition(new Partition(ul, br, pid));
	}

	// Stabbing query
	public int toPartitionId(final int[] c) {
		Set<Integer> ret = st[0].toPartitions(c[0]);

		for (int i = 1; i < nd; i++) {
			ret.retainAll(st[i].toPartitions(c[i]));
		}

		if (ret.size() != 1)
			throw new IllegalArgumentException("Point " + Arrays.toString(c) + " belongs to multiple pids or no pid: " + ret);

		return ret.toArray(new Integer[0])[0];
	}

	// Range query
	public int[] coveredPartitionIds(final int[] ul, final int[] br) {
		Set<Integer> ret = st[0].toPartitions(ul[0], br[0]);

		for (int i = 1; i < nd; i++) {
			ret.retainAll(st[i].toPartitions(ul[i], br[i]));
		}

		if (ret.size() < 1)
			throw new IllegalArgumentException("Rectangle <" + Arrays.toString(ul) + ", " + Arrays.toString(br) + "> covers no pid: " + ret);

		return ret.stream().mapToInt(i->i).toArray();
	}

	public static void main(String args[]) {
		DNonUniformPartition p = new DNonUniformPartition(new int[] {10, 20});

		p.insertPartition(new int[] {0, 0}, new int[] {3, 12}, 0);
		p.insertPartition(new int[] {0, 12}, new int[] {7, 20}, 1);
		p.insertPartition(new int[] {7, 8}, new int[] {10, 20}, 2);
		p.insertPartition(new int[] {3, 0}, new int[] {10, 8}, 3);
		p.insertPartition(new int[] {3, 8}, new int[] {7, 12}, 4);

		int[] c, c1, c2;

		c = new int[] {0, 0};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c = new int[] {4, 9};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c = new int[] {4, 12};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c = new int[] {7, 8};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c = new int[] {7, 5};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c1 = new int[] {3, 8};
		c2 = new int[] {7, 12};
		System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
		                   Arrays.toString(p.coveredPartitionIds(c1, c2)));

		c1 = new int[] {3, 8};
		c2 = new int[] {8, 12};
		System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
		                   Arrays.toString(p.coveredPartitionIds(c1, c2)));

		c1 = new int[] {3, 8};
		c2 = new int[] {7, 20};
		System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
		                   Arrays.toString(p.coveredPartitionIds(c1, c2)));

		c1 = new int[] {2, 8};
		c2 = new int[] {7, 20};
		System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
		                   Arrays.toString(p.coveredPartitionIds(c1, c2)));

		c1 = new int[] {2, 7};
		c2 = new int[] {8, 20};
		System.out.println("Rectangle <" + Arrays.toString(c1) + ", " + Arrays.toString(c2) + "> covers pids: " +
		                   Arrays.toString(p.coveredPartitionIds(c1, c2)));
	}
}

class AugmentedSegmentTree extends SegmentTree {

	public Set<Integer> toPartitions(int target) {
		List<Segment> res = contains(target);
		Set<Integer> s = new HashSet<Integer>();
		res.forEach(seg -> s.add(seg.pid));

		return s;
	}

	public Set<Integer> toPartitions(int st, int ed) {
		List<Segment> res = intersect(st, ed);
		Set<Integer> s = new HashSet<Integer>();
		res.forEach(seg -> s.add(seg.pid));

		return s;
	}
}
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

	// Field sizes, number of dimensions, number of LPs, and PID
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

	public void insertPartition(final int[] ul, final int[] br, final int pid) {
		insertPartition(new Partition(ul, br, pid));
	}

	public void insertPartition(Partition p) {
		for (int i = 0; i < nd; i++)
			st[i].insert(new Segment(p.ul[i], p.br[i], p.pid));
		ps.add(p);
	}

	public int toPartitionId(final int[] c) {
		Set<Integer> ret = st[0].toPartitions(c[0]);

		for (int i = 1; i < nd; i++) {
			ret.retainAll(st[i].toPartitions(c[i]));
		}

		if (ret.size() != 1)
			throw new IllegalArgumentException("Point " + Arrays.toString(c) + " belongs to multiple pids or no pid: " + ret);

		return ret.toArray(new Integer[0])[0];
	}

	public static void main(String args[]) {
		DNonUniformPartition p = new DNonUniformPartition(new int[]{10, 20});

		p.insertPartition(new int[]{0, 0}, new int[]{3, 12}, 0);
		p.insertPartition(new int[]{0, 12}, new int[]{7, 20}, 1);
		p.insertPartition(new int[]{7, 8}, new int[]{10, 20}, 2);
		p.insertPartition(new int[]{3, 0}, new int[]{10, 8}, 3);
		p.insertPartition(new int[]{3, 8}, new int[]{7, 12}, 4);

		int[] c; 

		c = new int[]{0, 0};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c = new int[]{4, 9};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c = new int[]{4, 12};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c = new int[]{7, 8};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));

		c = new int[]{7, 5};
		System.out.println("Point " + Arrays.toString(c) + " belongs to pid " + p.toPartitionId(c));
	}
}

class AugmentedSegmentTree extends SegmentTree {

	public Set<Integer> toPartitions(int target) {
        List<Segment> res = contains(target);
        Set<Integer> s = new HashSet<Integer>();
        res.forEach(seg -> s.add(seg.pid));

        return s;
    }
}
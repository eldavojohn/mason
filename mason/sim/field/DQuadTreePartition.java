package sim.field;

import java.util.*;
import java.util.stream.*;

import mpi.*;

import sim.util.*;

public class DQuadTreePartition extends DNonUniformPartition {
	QuadTree qt;
	QTNode myLeafNode; // the leaf node that this pid is mapped to
	Map<Integer, GroupComm> groups; // Map the level to its corresponding comm group

	public DQuadTreePartition(int[] size, boolean isToroidal) {
		super(size, isToroidal);
		qt = new QuadTree(new IntHyperRect(size), np);
	}

	// Mask these methods from DNonUniformPartition as they are not applicable to DQuadTreePartition
	@Override
	public void initUniformly(int[] dims) {
		throw new UnsupportedOperationException("initUniformly is not supported in DQuadTreePartition");
	}

	@Override
	public void insertPartition(IntHyperRect p) {
		throw new UnsupportedOperationException("insertPartition is not supported in DQuadTreePartition");
	}

	@Override
	public void removePartition(final int pid) {
		throw new UnsupportedOperationException("removePartition is not supported in DQuadTreePartition");
	}

	@Override
	public void updatePartition(IntHyperRect p) {
		throw new UnsupportedOperationException("updatePartition is not supported in DQuadTreePartition");
	}

	// hook createGroups() to setMPITopo
	@Override
	protected void setMPITopo() {
		try {
			createGroups();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		super.setMPITopo();
	}

	public void initQuadTree(List<IntPoint> splitPoints) {
		// Create the quad tree based on the given split points
		qt.split(splitPoints);

		// map all quad tree nodes to processors
		mapNodeToProc();

		// insert each region partitioned by the quad tree into the DNonUniformPartition structure
		for (QTNode node : qt.getAllLeaves()) {
			IntHyperRect rect = node.getShape();
			rect.setId(node.getProc());
			super.insertPartition(rect);
		}
	}

	protected void mapNodeToProc() {
		List<QTNode> leaves = qt.getAllLeaves();

		if (leaves.size() != np)
			throw new IllegalArgumentException("The number of leaves " + leaves.size() + " does not equal to the number of processors " + np);

		// Map the leaf nodes first
		for (int i = 0; i < np; i++)
			leaves.get(i).setProc(i);

		myLeafNode = leaves.get(pid);

		// Map non-leaf nodes - Use the first children node to hold itself
		while (leaves.size() > 0) {
			QTNode curr = leaves.remove(0), parent = curr.getParent();
			if (parent == null || parent.getChild(0) != curr)
				continue;
			parent.setProc(curr.getProc());
			leaves.add(parent);
		}
	}

	protected void createGroups() throws MPIException {
		int currDepth = 0;
		groups = new HashMap<Integer, GroupComm>();

		// Iterate level by level to create groups
		List<QTNode> currLevel = new ArrayList<QTNode>() {{ add(qt.getRoot()); }};
		while (currLevel.size() > 0) {
			List<QTNode> nextLevel = new ArrayList<QTNode>();

			for (QTNode node : currLevel) {
				nextLevel.addAll(node.getChildren());

				// whether this pid should participate in this group
				if (node.isAncestorOf(myLeafNode))
					groups.put(currDepth, new GroupComm(node));

				// Others will wait until the group is created
				MPI.COMM_WORLD.barrier();
			}

			currLevel = nextLevel;
			currDepth++;
		}
	}

	// Return whether the calling pid is the master node of the given GroupComm
	public boolean isGroupMaster(GroupComm gc) {
		return gc != null && gc.master.getProc() == pid;
	}

	// return the GroupComm instance if the calling pid should be involved
	// in the group communication of the given level
	// return null otherwise
	public GroupComm getGroupComm(int level) {
		return groups.get(level);
	}

	// return the shape when the calling pid holds one of the master nodes of this level
	// return null otherwise
	public IntHyperRect getNodeShapeAtLevel(int level) {
		GroupComm gc = getGroupComm(level);
		if (isGroupMaster(gc))
			return gc.master.getShape();
		return null;
	}

	private void testGroupComm(int depth) throws MPIException {
		if (groups.containsKey(depth)) {
			Comm gcomm = groups.get(depth).comm;
			int[] buf = new int[16];

			buf[gcomm.getRank()] = pid;
			gcomm.allGather(buf, 1, MPI.INT);
			System.out.println(String.format("PID %2d %s", pid, Arrays.toString(buf)));
		}

		MPI.COMM_WORLD.barrier();
	}

	public static void main(String[] args) throws MPIException {
		MPI.Init(args);

		DQuadTreePartition p = new DQuadTreePartition(new int[] {100, 100}, false);

		IntPoint[] splitPoints = new IntPoint[] {
		    new IntPoint(50, 50),
		    new IntPoint(25, 25),
		    new IntPoint(75, 75),
		    new IntPoint(60, 90),
		    new IntPoint(10, 10)
		};

		p.initQuadTree(Arrays.asList(splitPoints));
		p.commit();

		// System.out.println(p.root);

		// for (QTNode node : p.root.getAllLeaves())
		// 	System.out.println("Leaf " + node.getId());

		//p.setMPITopo();

		// System.out.println("------------");
		// System.out.println(p.pnMap);
		// System.out.println("------------");
		// System.out.println(p.npMap);

		// sim.util.MPITest.printInOrder(p.getPartition().toString());

		// sim.util.MPITest.printInOrder(Arrays.toString(p.getNeighborIds()));

		p.testGroupComm(2);

		MPI.Finalize();
	}
}
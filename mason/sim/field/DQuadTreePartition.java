package sim.field;

import java.util.*;
import java.util.stream.*;

import mpi.*;

import sim.util.*;

public class DQuadTreePartition extends DNonUniformPartition {
	final int gpsize;

	QTNode root;

	// Map one quad tree node id to one processor id
	Map<Integer, Integer> npMap;

	// Map one processor id to potentially many quad tree node ids
	Map<Integer, ArrayList<Integer>> pnMap;

	// Map the level to its corresponding comm group
	Map<Integer, GroupComm> groups;

	public DQuadTreePartition(int[] size, boolean isToroidal) {
		super(size, isToroidal);

		IntHyperRect shape = new IntHyperRect(-1, new IntPoint(new int[nd]), new IntPoint(size));

		root = new QTNode(shape, np);
		gpsize = 1 << nd;
	}

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
		splitPoints.stream().forEach(p -> root.getLeafNode(p).split(p));

		mapNodeToProc();

		for(QTNode node : root.getAllLeaves()) {
			IntHyperRect rect = node.getShape();
			rect.setId(npMap.get(node.getId()));
			super.insertPartition(rect);
		}
	}

	protected void mapNodeToProc() {
		pnMap = new HashMap<Integer, ArrayList<Integer>>();
		npMap = new HashMap<Integer, Integer>();

		List<QTNode> leaves = root.getAllLeaves();

		if (leaves.size() != np)
			throw new IllegalArgumentException("The number of leaves " + leaves.size() + " does not equal to np " + np);

		// Map the leaf nodes first
		for (int i = 0; i < np; i++) {
			int nid = leaves.get(i).getId();
			pnMap.put(i, new ArrayList<Integer>() {{ add(nid); }});
			npMap.put(nid, i);
		}

		// Map non-leaf nodes - Use the first children node to hold itself
		for (QTNode node : leaves) {
			QTNode curr = node;
			while (curr.getParent() != null && curr.getParent().getChildren().get(0) == curr) {
				int myPid = npMap.get(node.getId());
				int parentNid = curr.getParent().getId();

				pnMap.get(myPid).add(parentNid);
				npMap.put(parentNid, myPid);

				curr = curr.getParent();
			}
		}
	}

	protected void createGroups() throws MPIException {
		int currDepth = 0;
		groups = new HashMap<Integer, GroupComm>();

		// Iterate level by level to create groups
		List<QTNode> curr = new ArrayList<QTNode>() {{ add(root); }};
		while (curr.size() > 0) {
			List<QTNode> next = new ArrayList<QTNode>();

			while (curr.size() > 0) {
				List<QTNode> children = curr.remove(0).getChildren();

				GroupComm gc = createGroup(children);
				if (gc != null)
					groups.put(currDepth, gc);
				MPI.COMM_WORLD.barrier();

				next.addAll(children);
			}

			curr = next;
			currDepth++;
		}
	}

	class GroupComm {
		// MPI Communicator for the group
		Comm comm;

		// Node id in the quad tree
		int nodeId;

		// The global pid of the group root, the local pid of the group root
		int rootPid, rootGid;

		public GroupComm(Comm comm, int nodeId, int rootPid, int rootGid) {
			this.comm = comm;
			this.nodeId = nodeId;
			this.rootPid = rootPid;
			this.rootGid = rootGid;
		}
	}

	protected GroupComm createGroup(List<QTNode> nodes) throws MPIException {
		int myNodeId = -1;
		int[] pids = new int[nodes.size()];

		// Get the corresponding pids that are in charge of the given nodes
		for (int i = 0; i < nodes.size(); i++) {
			pids[i] = npMap.get(nodes.get(i).getId());
			if (pids[i] == pid)
				myNodeId = nodes.get(i).getId();
		}

		// if the pid is not inside this group, return null
		if (myNodeId == -1)
			return null;

		// Create a group communicator between these pids
		Group world = MPI.COMM_WORLD.getGroup(), myGroup = world.incl(pids);
		Comm gcomm = MPI.COMM_WORLD.createGroup(myGroup, 0);

		int rootPid = npMap.get(root.getNode(myNodeId).getParent().getId());
		int rootGid = Group.translateRanks(world, new int[] {rootPid}, myGroup)[0];

		return new GroupComm(gcomm, myNodeId, rootPid, rootGid);
	}

	public void testGroupComm(int depth) throws MPIException {
		if (groups.containsKey(depth)) {
			Comm gcomm = groups.get(depth).comm;
			int[] buf = new int[16];

			buf[gcomm.getRank() * 4 + 0] = pid;
			buf[gcomm.getRank() * 4 + 1] = gcomm.getRank();
			buf[gcomm.getRank() * 4 + 2] = groups.get(depth).rootPid;
			buf[gcomm.getRank() * 4 + 3] = groups.get(depth).rootGid;

			gcomm.allGather(buf, 4, MPI.INT);
			System.out.println("PID " + pid + " " + Arrays.toString(buf));
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

		sim.util.MPITest.printInOrder(p.getPartition().toString());

		sim.util.MPITest.printInOrder(Arrays.toString(p.getNeighborIds()));

		//p.testGroupComm(2);

		//p.testGroupComm(1);

		//p.testGroupComm(2);

		MPI.Finalize();
	}
}
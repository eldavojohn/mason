package sim.util;

import java.util.*;
import java.util.stream.*;

public class QuadTree {
	int depth = 0;

	QTNode root;
	List<Integer> availIds;
	Map<Integer, QTNode> allNodes;

	// Shape of field and the maximum number of partitions it can hold
	public QuadTree(IntHyperRect shape, int np) {
		final int div = 1 << shape.getNd();

		if (np % (div - 1) != 1)
			throw new IllegalArgumentException("the number of processors/regions is illegal");

		root = new QTNode(shape, null);
		availIds = IntStream.range(1, np / (div - 1) * div + 1).boxed().collect(Collectors.toList());
		allNodes = new HashMap<Integer, QTNode>() {{ put(0, root); }};
	}

	public int getDepth() {
		return depth;
	}

	public QTNode getRoot() {
		return root;
	}

	public QTNode getNode(int id) {
		return allNodes.get(id);
	}

	public QTNode getLeafNode(IntPoint p) {
		return root.getLeafNode(p);
	}

	public List<QTNode> getAllNodes() {
		return new ArrayList<QTNode>(allNodes.values());
	}

	public List<QTNode> getAllLeaves() {
		return allNodes.values().stream().filter(node -> node.isLeaf()).collect(Collectors.toList());
	}

	public String toString() {
		return root.toStringAll();
	}

	public void split(IntPoint p) {
		root.getLeafNode(p).split(p).forEach(x -> addNode(x));
	}

	public void split(List<IntPoint> ps) {
		ps.forEach(p -> split(p));
	}

	public void moveOrigin(QTNode node, IntPoint newOrig) {
		node.split(newOrig).forEach(x -> addNode(x));;
	}

	public void moveOrigin(int id, IntPoint newOrig) {
		moveOrigin(getNode(id), newOrig);
	}

	public void merge(QTNode node) {
		node.merge().forEach(x -> delNode(x));
	}

	public void merge(int id) {
		merge(getNode(id));
	}

	protected void addNode(QTNode node) {
		if (availIds.size() == 0)
			throw new IllegalArgumentException("Reached maximum number of regions, cannot add more child");

		int id = availIds.remove(0);
		node.setId(id);
		allNodes.put(id, node);
		depth = Math.max(depth, node.getLevel());
	}

	protected void delNode(QTNode node) {
		int id = node.getId();
		allNodes.remove(id);
		availIds.add(id);
		if (depth == node.getLevel())
			depth = allNodes.values().stream().mapToInt(x -> x.getLevel()).max().orElse(0);
	}

	public static void main(String[] args) {
		IntHyperRect field = new IntHyperRect(-1, new IntPoint(0, 0), new IntPoint(100, 100));

		QuadTree qt = new QuadTree(field, 7);

		qt.split(new IntPoint(40, 60));
		System.out.println(qt);

		qt.split(new IntPoint(10, 80));
		System.out.println(qt);

		IntPoint p1 = new IntPoint(50, 50);
		System.out.println("Point " + p1 + " is in node " + qt.getLeafNode(p1));

		qt.moveOrigin(qt.getRoot(), new IntPoint(60, 70));
		System.out.println(qt);

		System.out.println("Point " + p1 + " is in node " + qt.getLeafNode(p1));

		System.out.println("------------");
		System.out.println(qt.availIds);
		for (QTNode node : qt.allNodes.values())
			System.out.println(node);
		System.out.println(qt.depth);

		System.out.println("Merge one of root's children");
		qt.merge(qt.getRoot().getChild(1));
		System.out.println(qt.availIds);
		for (QTNode node : qt.getAllNodes())
			System.out.println("Node " + node);
		for (QTNode node : qt.getAllLeaves())
			System.out.println("Leaf " + node);
		System.out.println(qt.depth);

		System.out.println("Merge root");
		qt.merge(qt.getRoot());
		System.out.println(qt.availIds);
		for (QTNode node : qt.getAllNodes())
			System.out.println("Node " + node);
		System.out.println(qt.depth);
	}
}
package sim.util;

import java.util.*;
import java.util.stream.*;

// TODO Divide this into QTNode class and QuadTree class
// TODO Currently all shapes are restricted to IntHyperRect - switch to NdRectangle once it is completed
public class QTNode {

	final int nd;
	int level, id, pos; // which level in the tree the node is in, its node id, and the position in its parent's children list

	IntPoint origin;
	IntHyperRect shape;

	QTNode parent;
	List<QTNode> children;
	
	Map<Integer, QTNode> allNodes;
	List<Integer> availIds;

	// Shape of field and the maximum number of partitions it can hold
	public QTNode(IntHyperRect shape, int np) {
		this.nd = shape.getNd();
		this.shape = shape;

		final int div = 1 << nd;
		if (np % (div - 1) != 1)
			throw new IllegalArgumentException("the number of processors/regions is illegal");

		this.children = new ArrayList<QTNode>();
		this.availIds = IntStream.range(1, np / (div - 1) * div + 1).boxed().collect(Collectors.toList());
		this.allNodes = new HashMap<Integer, QTNode>();

		this.id = 0;
		this.level = 0;
		this.parent = null;

		allNodes.put(0, this);
	}

	protected QTNode(IntHyperRect shape) {
		this.nd = shape.getNd();
		this.shape = shape;
		this.children = new ArrayList<QTNode>();
	}

	public int getLevel() {
		return level;
	}

	public IntPoint getOrigin() {
		return origin;
	}

	public IntHyperRect getShape() {
		return shape;
	}

	public List<QTNode> getChildren() {
		return children;
	}

	public QTNode getParent() {
		return parent;
	}

	public int getId() {
		return id;
	}

	public boolean isRoot() {
		return parent == null;
	}

	public boolean isLeaf() {
		return children.size() == 0;
	}

	public QTNode getNode(int id) {
		return allNodes.get(id);
	}

	// Get the siblings (including itself) of the given node id
	public List<QTNode> getSiblings(int id) {
		QTNode node = allNodes.get(id);

		if(node.getParent() == null)
			return new ArrayList<QTNode>() {{ add(node); }};

		return node.getParent().getChildren();
	}

	public List<QTNode> getAllNodes() {
		return new ArrayList<QTNode>(allNodes.values());
	}

	public List<QTNode> getAllLeaves() {
		return allNodes.values().stream().filter(node -> node.isLeaf()).collect(Collectors.toList());
	}

	public List<QTNode> split(IntPoint newOrigin) {
		if (!shape.contains(newOrigin))
			throw new IllegalArgumentException("newOrigin " + newOrigin + " is outside the region " + shape);

		origin = newOrigin;

		if (isLeaf())
			children = IntStream.range(0, 1 << nd)
			           .mapToObj(i -> getNewChild(getChildShape(i), i))
			           .collect(Collectors.toList());
		else
			for (int i = 0; i < children.size(); i++)
				children.get(i).reshape(getChildShape(i));

		return children;
	}

	protected QTNode getNewChild(IntHyperRect shape, int pos) {
		if (availIds.size() == 0)
			throw new IllegalArgumentException("Reached maximum number of regions, cannot add more child");

		QTNode child = new QTNode(shape);

		child.availIds = availIds;
		child.allNodes = allNodes;

		child.id = availIds.remove(0);
		child.level = level + 1;
		child.parent = this;
		child.pos = pos;

		allNodes.put(child.id, child);

		return child;
	}

	public void merge() {
		if (isLeaf())
			return;

		for (QTNode child : children) {
			child.merge();
			allNodes.remove(child.id);
			availIds.add(child.id);
		}

		children.clear();
		origin = null;
		pos = -1;
	}

	public QTNode getChildNode(IntPoint p) {
		return children.get(toChildIdx(p));
	}

	public QTNode getLeafNode(IntPoint p) {
		QTNode curr = this;

		while (!curr.isLeaf())
			curr = curr.getChildNode(p);

		return curr;
	}

	protected void reshape(IntHyperRect newShape) {
		shape = newShape;
		for (int i = 0; i < children.size(); i++)
			children.get(i).reshape(getChildShape(i));
	}

	protected IntHyperRect getChildShape(int childId) {
		int[] ul = shape.ul().getArray();
		int[] br = origin.getArray();
		int[] sbr = shape.br().getArray();

		for (int i = 0; i < nd; i++)
			if (((childId >> (nd - i - 1)) & 0x1) == 1) {
				ul[i] = br[i];
				br[i] = sbr[i];
			}

		return new IntHyperRect(-1, new IntPoint(ul), new IntPoint(br));
	}

	protected int toChildIdx(IntPoint p) {
		if (!shape.contains(p))
			throw new IllegalArgumentException("p " + p + " must be inside the shape " + shape);

		double[] oc = origin.getArrayInDouble(), pc = p.getArrayInDouble();

		return IntStream.range(0, nd)
		       .map(i -> pc[i] < oc[i] ? 0 : 1)
		       .reduce(0, (r, x) -> r << 1 | x);
	}

	public String toString() {
		return toStringRecursive(new StringBuffer("QTNode\n"), "", true).toString();
	}

	protected StringBuffer toStringRecursive(StringBuffer buf, String prefix, boolean isTail) {
		buf.append(prefix +
		           (isTail ? "└── " : "├── ") + "ID " + id + " " + shape +
		           (origin == null ? "\n" : (" Origin " + origin + "\n"))
		          );

		for (int i = 0; i < children.size() - 1; i++)
			children.get(i).toStringRecursive(buf, prefix + (isTail ? "    " : "│   "), false);

		if (children.size() > 0)
			children.get(children.size() - 1).toStringRecursive(buf, prefix + (isTail ? "    " : "│   "), true);

		return buf;
	}

	public static void main(String[] args) {
		IntHyperRect field = new IntHyperRect(-1, new IntPoint(0, 0), new IntPoint(100, 100));
		QTNode root = new QTNode(field, 7);

		root.split(new IntPoint(40, 60));
		System.out.println(root);

		root.children.get(1).split(new IntPoint(10, 80));
		System.out.println(root);

		IntPoint p1 = new IntPoint(50, 50);
		System.out.println("Point " + p1 + " is in node id " + root.getLeafNode(p1).id);

		root.split(new IntPoint(60, 70));
		System.out.println(root);

		System.out.println("Point " + p1 + " is in node id " + root.getLeafNode(p1).id);

		System.out.println("------------");
		System.out.println(root.availIds);
		for (QTNode node : root.allNodes.values())
			System.out.println(node.id);

		System.out.println("Merge one of root's children");
		root.children.get(1).merge();
		System.out.println(root.availIds);
		for (QTNode node : root.getAllNodes())
			System.out.println("Node " + node.id);
		for (QTNode node : root.getAllLeaves())
			System.out.println("Leaf " + node.id);

		System.out.println("Merge root");
		root.merge();
		System.out.println(root.availIds);
		for (QTNode node : root.getAllNodes())
			System.out.println("Node " + node.id);
		for (QTNode node : root.getAllLeaves())
			System.out.println("Leaf " + node.id);
	}
}
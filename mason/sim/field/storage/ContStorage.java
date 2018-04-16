package sim.field.storage;

import java.io.Serializable;
import java.util.*;
import java.util.stream.*;
import java.util.function.*;

import sim.util.*;
import sim.field.storage.GridStorage;

public class ContStorage<T extends Serializable> extends GridStorage {

	int[] discretizations, dsize;
	HashMap<T, NdPoint> m;

	public ContStorage(IntHyperRect shape, int[] discretizations) {
		super(shape);

		this.discretizations = discretizations;
		this.storage = allocate(shape.getArea());
	}

	protected Object allocate(int size) {
		this.dsize = IntStream.range(0, shape.nd).map(i -> shape.getSize()[i] / discretizations[i]).toArray();
		// Overwrite the original stride with the new stride of dsize;
		// so that getFlatIdx() can correctly get the cell index of a discretized point
		// TODO better approach?
		this.stride = getStride(dsize);
		this.m = new HashMap<T, NdPoint>();
		return IntStream.range(0, size / Arrays.stream(discretizations).reduce(1, (x, y) -> x * y))
		       .mapToObj(i -> new HashSet()).toArray(s -> new HashSet[s]);
	}

	public String toString() {
		HashSet<T>[] cells = (HashSet<T>[])storage;

		StringBuffer buf = new StringBuffer(String.format("ContStorage-%s\n", shape));

		for (IntPoint p : IntPointGenerator.getBlock(dsize))
			buf.append("Cell " + p + ":\t" + cells[getFlatIdx(p)] + "\n");

		return buf.toString();
	}

	public Serializable pack(MPIParam mp) {
		ArrayList objs = new ArrayList();

		for (IntHyperRect rect : mp.rects) 
			// shift the rect with local coordinates back to global coordinates
			for (T obj : getObjects(rect.shift(shape.ul.c))) {
				// put the object itself and its location
				objs.add(obj);
				objs.add(m.get(obj));
			}

		return objs.toArray();
	}

	public int unpack(MPIParam mp, Serializable buf) {
		Object[] objs = (Object[])buf;

		for (int i = 0; i < objs.length; i += 2)
			putObject((T)objs[i], (NdPoint)objs[i + 1]);

		return objs.length / 2;
	}

	protected IntPoint discretize(final NdPoint p) {
		double[] offsets = shape.ul.getOffsetsDouble(p);
		return new IntPoint(IntStream.range(0, offsets.length)
		                    .map(i -> -(int)offsets[i] / discretizations[i])
		                    .toArray());
	}

	public List<T> getObjects(final NdPoint p) {
		HashSet<T>[] cells = (HashSet<T>[])storage;
		return cells[getFlatIdx(discretize(p))].stream()
		       .filter(obj -> m.get(obj).equals(p))
		       .collect(Collectors.toList());
	}

	public List<T> getObjects(final IntHyperRect r) {
		ArrayList<T> objs = new ArrayList<T>();
		HashSet<T>[] cells = (HashSet<T>[])storage;

		for (IntPoint p : IntPointGenerator.getBlock(discretize(r.ul), discretize(r.br)))
			cells[getFlatIdx(p)].stream()
			.filter(obj -> r.contains(m.get(obj)))
			.forEach(obj -> objs.add(obj));

		return objs;
	}

	public void putObject(final T obj, final NdPoint p) {
		HashSet<T>[] cells = (HashSet<T>[])storage;
		m.put(obj, p);
		cells[getFlatIdx(discretize(p))].add(obj);
	}

	public void removeObject(final T obj) {
		HashSet<T>[] cells = (HashSet<T>[])storage;
		cells[getFlatIdx(discretize(m.get(obj)))].remove(obj);
	}

	public void removeObjects(final NdPoint p) {
		for (T obj : getObjects(p))
			removeObject(obj);
	}

	public NdPoint getLocation(final T obj) {
		return m.get(obj);
	}

	public List<T> getNearestNeighbors(final T obj, final int need) {
		final int nd = shape.nd;
		final NdPoint loc = m.get(obj);
		final IntPoint dloc = discretize(loc);
		final ArrayList<T> objs = new ArrayList<T>();
		final HashSet<T>[] cells = (HashSet<T>[])storage;
		final ArrayList<T> candidates = new ArrayList<T>(cells[getFlatIdx(dloc)]);
		final int maxLayer = IntStream.range(0, nd)
		                     .map(i -> Math.max(dloc.c[i], dsize[i] - dloc.c[i]))
		                     .max().getAsInt();

		int currLayer = 1;
		candidates.remove(obj); // remove self

		while (objs.size() < need && currLayer <= maxLayer) {
			for (IntPoint p : IntPointGenerator.getLayer(dloc, currLayer))
				if (IntStream.range(0, nd).allMatch(i -> p.c[i] >= 0 && p.c[i] < dsize[i]))
					candidates.addAll(cells[getFlatIdx(p)]);

			if (candidates.size() + objs.size() >= need) {
				candidates.sort(Comparator.comparingDouble(o -> m.get(o).getDistance(loc, 2)));
				objs.addAll(candidates.subList(0, need - objs.size()));
				break;
			} else
				objs.addAll(candidates);

			candidates.clear();
			currLayer++;
		}

		return objs;
	}

	public List<T> getNeighborsWithin(final T obj, final double radius) {
		final int nd = shape.nd;
		final NdPoint loc = m.get(obj);
		final IntPoint dloc = discretize(loc);
		final ArrayList<T> objs = new ArrayList<T>();
		final HashSet<T>[] cells = (HashSet<T>[])storage;

		// Calculate how many discretized cells we need to search
		final int[] offsets = Arrays.stream(discretizations).map(x -> (int)Math.ceil(radius / x)).toArray();

		// Generate the start/end point subject to the boundaries
		final IntPoint ul = new IntPoint(IntStream.range(0, nd).map(i -> Math.max(dloc.c[i] - offsets[i], 0)).toArray());
		final IntPoint br = new IntPoint(IntStream.range(0, nd).map(i -> Math.min(dloc.c[i] + offsets[i] + 1, dsize[i])).toArray());

		// Collect all the objects that are not obj itself and within the given radius
		for (IntPoint p : IntPointGenerator.getBlock(ul, br))
			cells[getFlatIdx(p)].stream()
			.filter(x -> x != obj && m.get(x).getDistance(loc, 2) <= radius)
			.forEach(x -> objs.add(x));

		return objs;
	}

	public static void main(String[] args) throws mpi.MPIException {
		mpi.MPI.Init(args);

		IntPoint ul = new IntPoint(10, 20), br = new IntPoint(50, 80);
		IntHyperRect rect = new IntHyperRect(1, ul, br);
		int[] discretize = new int[] {10, 10};

		ContStorage<TestObj> f = new ContStorage<TestObj>(rect, discretize);

		TestObj obj1 = new TestObj(1); DoublePoint loc1 = new DoublePoint(23.4, 30.2);
		TestObj obj2 = new TestObj(2); DoublePoint loc2 = new DoublePoint(29.99, 39.99);
		TestObj obj3 = new TestObj(3); DoublePoint loc3 = new DoublePoint(31, 45.6);
		TestObj obj4 = new TestObj(4); DoublePoint loc4 = new DoublePoint(31, 45.6);
		TestObj obj5 = new TestObj(5); DoublePoint loc5 = new DoublePoint(31, 45.60001);

		f.putObject(obj1, loc1);
		f.putObject(obj2, loc2);
		f.putObject(obj3, loc3);
		f.putObject(obj4, loc4);
		f.putObject(obj5, loc5);

		System.out.println("get objects at " + loc1);
		for (TestObj obj : f.getObjects(loc1))
			System.out.println(obj);

		System.out.println("get objects at " + loc4);
		for (TestObj obj : f.getObjects(loc4))
			System.out.println(obj);

		System.out.println("get objects at " + loc5);
		for (TestObj obj : f.getObjects(loc5))
			System.out.println(obj);

		IntHyperRect r1 = new IntHyperRect(-1, new IntPoint(20, 30), new IntPoint(31, 41));
		System.out.println("get objects in " + r1);
		for (TestObj obj : f.getObjects(r1))
			System.out.println(obj);

		for (int count = 1; count <= 5; count++) {
			System.out.println("get " + count + " neighbors of " + obj2);
			for (TestObj obj : f.getNearestNeighbors(obj2, count))
				System.out.println(obj);
		}

		double r = 9;
		System.out.println("get objects within " + r + " from " + obj2);
		for (TestObj obj : f.getNeighborsWithin(obj2, r))
			System.out.println(obj);

		r = 12;
		System.out.println("get objects within " + r + " from " + obj2);
		for (TestObj obj : f.getNeighborsWithin(obj2, r))
			System.out.println(obj);

		IntHyperRect r2 = new IntHyperRect(-1, new IntPoint(20, 30), new IntPoint(31, 41));
		System.out.println("after remove " + obj1 + ", get objects in " + r2);
		f.removeObject(obj1);
		for (TestObj obj : f.getObjects(r2))
			System.out.println(obj);

		System.out.println("after remove object at " + loc3 + ", get objects in " + rect);
		f.removeObjects(loc3);
		for (TestObj obj : f.getObjects(rect))
			System.out.println(obj);

		f.putObject(obj1, loc1);
		f.putObject(obj3, loc3);
		f.putObject(obj4, loc4);
		System.out.println("after putting " + obj1 + " " + obj3 + " " + obj4 + " " + "back, get objects in " + rect);
		System.out.println(f);

		IntPoint ul2 = new IntPoint(20, 30), br2 = new IntPoint(30, 40);
		IntHyperRect rect2 = new IntHyperRect(2, ul2, br2);
		f.reshape(rect2);
		System.out.println("after reshaping from " + rect + " to " + rect2 + ", get objects in " + rect2);
		System.out.println(f);

		r = 12;
		System.out.println("get objects within " + r + " from " + obj1);
		for (TestObj obj : f.getNeighborsWithin(obj1, r))
			System.out.println(obj);

		System.out.println("get objects at " + loc1);
		for (TestObj obj : f.getObjects(loc1))
			System.out.println(obj);

		mpi.MPI.Finalize();
	}
}
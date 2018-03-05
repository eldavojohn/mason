package sim.util;

import java.util.*;

public class SegmentTree {

    public Segment root;
    public final boolean isToroidal;

    public SegmentTree() {
        this(false);
    }

    public SegmentTree(final boolean isToroidal) {
        this.isToroidal = isToroidal;
    }

    public boolean all() {
        if (root == null)
            return true;
        return root.all();
    }

    public void insert(double st, double ed) {
        this.insert(new Segment(st, ed));
    }

    public void insert(Segment target) {
        if (root == null)
            root = target;
        else
            insert(root, target);
    }

    protected void insert(Segment curr, Segment target) {
        if (curr == null) {
            curr = target;
            return;
        }

        if (target.ed > curr.max)
            curr.max = target.ed;

        if (target.st < curr.min)
            curr.min = target.st;

        if (curr.compareTo(target) <= 0) {
            if (curr.right == null) {
                curr.right = target;
            } else {
                insert(curr.right, target);
            }
        } else {
            if (curr.left == null) {
                curr.left = target;
            } else {
                insert(curr.left, target);
            }
        }
    }

    public void print() {
        System.out.println("Segment Tree:");
        print(root);
        System.out.println();
    }

    protected void print(Segment curr) {
        if (curr == null) {
            System.out.print("null ");
            return;
        }

        System.out.print(curr + " ");
        print(curr.left);
        print(curr.right);
    }

    public List<Segment> intersect(double st, double ed) {
        return this.intersect(new Segment(st, ed));
    }

    public List<Segment> intersect(Segment target) {
        List<Segment> res = new ArrayList<Segment>();
        if (isToroidal)
            generate(target).forEach(seg -> intersect(root, seg, res));
        else
            intersect(root, target, res);
        return res;
    }

    protected void intersect(Segment curr, Segment target, List<Segment> res) {
        if (curr == null)
            return;

        if (curr.overlapWith(target))
            res.add(curr);

        if ((curr.left != null) && (curr.left.max >= target.st))
            this.intersect(curr.left, target, res);

        this.intersect(curr.right, target, res);
    }

    public List<Segment> contains(double target) {
        List<Segment> res = new ArrayList<Segment>();
        if (isToroidal)
            contains(root, conv(target), res);
        else
            contains(root, target, res);
        return res;
    }

    protected void contains(Segment curr, double target, List<Segment> res) {
        if (curr == null)
            return;

        if (curr.contains(target))
            res.add(curr);

        if ((curr.left != null) && (curr.left.max >= target))
            this.contains(curr.left, target, res);

        this.contains(curr.right, target, res);
    }

    /**
     *  In case of toroidal, Generate new segments based on the following rules (tab size=4)
     *  'x' mean impossible case
     *  ----------------------------------------------------------------------------------------------------------------
     *                  |   <min        |   =min        |   min < ed < max      |   =max        |   >max
     *  ----------------------------------------------------------------------------------------------------------------
     *              <min |gen(nst,ned)  |   (nst,max)   |   (nst,max),(min,ed)  |   (st,ed)     |   (st,ed)
     *              =min |      x       |   ()          |   (st,ed)             |   (st,ed)     |   (st,ed)
     *    min < st < max |      x       |       x       |   (st,ed)             |   (st,ed)     |   (st,max),(min,ned)
     *              =max |      x       |       x       |       x               |   ()          |   (min,ned)
     *              >max |      x       |       x       |       x               |       x       |   gen(nst,ned)
     *  ----------------------------------------------------------------------------------------------------------------
    **/
    private List<Segment> generate(Segment orig) {
        final double st = orig.st, ed = orig.ed;
        final double min = root.min, max = root.max, len = max - min;
        List<Segment> ret = new ArrayList<Segment>();

        if (ed < min) {
            double ned = conv(ed);
            return generate(new Segment(st + ned - ed, ned));
        } else if (st > max) {
            double nst = conv(st);
            return generate(new Segment(nst, ed + nst - st));
        } else if (ed == max || (min < ed && ed < max && min <= st && st < max) || (ed > max && st <= min))
            ret.add(orig);
        else if (ed == min && st < min)
            ret.add(new Segment(conv(st), max));
        else if (ed > max && st == max)
            ret.add(new Segment(min, conv(ed)));
        else if (st < min && min < ed && ed < max) {
            ret.add(new Segment(conv(st), max));
            ret.add(new Segment(min, ed));
        } else if (ed > max && min < st && st < max) {
            ret.add(new Segment(st, max));
            ret.add(new Segment(min, conv(ed)));
        }

        return ret;
    }

    // Convert the value in case of toroidal
    private double conv(final double val) {
        final double len = root.max - root.min;
        if (val < root.min)
            return root.max - (root.min - val) % len;
        else if (val > root.max)
            return root.min + (val - root.max) % len;
        return val;
    }

    public static void main(String[] args) {
        System.out.println("Testing SegmentTree\n");
        testNormal();
        System.out.println("\nTesting SegmentTreeToroidal\n");
        testToroidal();
    }

    public static void testNormal() {
        List<Segment> res;
        SegmentTree t = new SegmentTree();

        t.insert(4, 10);
        t.insert(10, 11);
        t.insert(13, 15);
        t.insert(1, 7);
        t.insert(6, 9);
        t.insert(5, 8);
        t.insert(8, 12);
        t.insert(9, 20);

        t.print();

        res = t.intersect(7, 10);
        System.out.println("Intersect [7, 10] \nResult: " + Arrays.toString(res.toArray()));

        res = t.intersect(1, 1);
        System.out.println("Intersect [1, 1] \nResult: " + Arrays.toString(res.toArray()));

        res = t.contains(9);
        System.out.println("Contains 9 \nResult: " + Arrays.toString(res.toArray()));

        System.out.println("All: " + t.all());

        System.out.println("\nT2...");

        SegmentTree t2 = new SegmentTree();

        t2.insert(4, 10);
        System.out.println("All: " + t2.all());

        t2.insert(12, 15);
        System.out.println("All: " + t2.all());

        t2.insert(1, 3);
        System.out.println("All: " + t2.all());

        t2.insert(8, 12);
        System.out.println("All: " + t2.all());

        t2.insert(0, 5);
        System.out.println("All: " + t2.all());
    }

    public static void testToroidal() {
        List<Segment> res;
        SegmentTree t = new SegmentTree(true);

        t.insert(new Segment(2, 4, 0));
        t.insert(new Segment(3, 5, 1));
        t.insert(new Segment(3, 7, 2));
        t.insert(new Segment(6, 8, 3));
        t.insert(new Segment(9, 10, 4));
        t.insert(new Segment(5, 6, 5));

        List<Double> testVals = Arrays.asList(-10.0, 1.5, 4.0, 7.5, 11.5, 100.0);
        List<List<Integer>> expected1 = Arrays.asList(
                                           Arrays.asList(2, 3), Arrays.asList(4), Arrays.asList(1, 2), 
                                           Arrays.asList(3), Arrays.asList(0, 1, 2), Arrays.asList(1, 2)
                                       );

        for (int i = 0; i < testVals.size(); i++) {
            double val = testVals.get(i);
            res = t.contains(val);
            Set<Integer> want = new HashSet<Integer>(expected1.get(i));
            Set<Integer> got = new HashSet<Integer>(res.stream().mapToInt(x->x.pid).boxed().collect(java.util.stream.Collectors.toList()));
            System.out.println("Contains " + val + "\tGot: " + got + " Want: " + want);
            assert want.equals(got);
        }

        List<Segment> testSegs = Arrays.asList(
                                     new Segment(-2, -1), new Segment(-2, 2), new Segment(-2, 5), new Segment(-2, 10), new Segment(-2, 100),
                                     new Segment(2, 2), new Segment(2, 6), new Segment(2, 10), new Segment(2, 12),
                                     new Segment(4, 7), new Segment(4, 10), new Segment(4, 19),
                                     new Segment(10, 10), new Segment(10, 14),
                                     new Segment(15, 25)
                                 );

        List<List<Integer>> expected2 = Arrays.asList(
                                           Arrays.asList(2, 3), Arrays.asList(2, 3, 4), Arrays.asList(0, 1, 2, 3, 4), Arrays.asList(0, 1, 2, 3, 4, 5), Arrays.asList(0, 1, 2, 3, 4, 5),
                                           Arrays.asList(), Arrays.asList(0, 1, 2, 5), Arrays.asList(0, 1, 2, 3, 4, 5), Arrays.asList(0, 1, 2, 3, 4, 5),
                                           Arrays.asList(1, 2, 3, 5), Arrays.asList(1, 2, 3, 4, 5), Arrays.asList(0, 1, 2, 3, 4, 5),
                                           Arrays.asList(), Arrays.asList(0, 1, 2, 5),
                                           Arrays.asList(0, 1, 2, 3, 4, 5)
                                       );

        for (int i = 0; i < testSegs.size(); i++) {
            Segment s = testSegs.get(i);
            res = t.intersect(s);
            Set<Integer> want = new HashSet<Integer>(expected2.get(i));
            Set<Integer> got = new HashSet<Integer>(res.stream().mapToInt(x->x.pid).boxed().collect(java.util.stream.Collectors.toList()));
            System.out.println("Intersect " + s + "\tGot: " + got + " Want: " + want);
            assert want.equals(got);
        }
    }
}

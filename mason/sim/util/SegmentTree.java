package sim.util;

import java.util.*;

public class SegmentTree {

    public Segment root;

    public boolean all() {
        if (root == null)
            return true;
        return root.all();
    }

    public void insert(int st, int ed) {
        this.insert(new Segment(st, ed));
    }

    public void insert(Segment target) {
        if (root == null)
            root = target;
        else
            insert(root, target);
    }

    private void insert(Segment curr, Segment target) {
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

    private void print(Segment curr) {
        if (curr == null) {
            System.out.print("null ");
            return;
        }

        System.out.print(curr + " ");
        print(curr.left);
        print(curr.right);
    }

    public List<Segment> intersect(int st, int ed) {
        return this.intersect(new Segment(st, ed));
    }

    public List<Segment> intersect(Segment target) {
        List<Segment> res = new ArrayList<Segment>();
        intersect(root, target, res);
        return res;
    }

    private void intersect(Segment curr, Segment target, List<Segment> res) {
        if (curr == null)
            return;

        if (curr.overlapWith(target))
            res.add(curr);

        if ((curr.left != null) && (curr.left.max >= target.st))
            this.intersect(curr.left, target, res);

        this.intersect(curr.right, target, res);
    }

    public List<Segment> contains(int target) {
        List<Segment> res = new ArrayList<Segment>();
        contains(root, target, res);
        return res;
    }

    private void contains(Segment curr, int target, List<Segment> res) {
        if (curr == null)
            return;

        if (curr.contains(target))
            res.add(curr);

        if ((curr.left != null) && (curr.left.max >= target))
            this.contains(curr.left, target, res);

        this.contains(curr.right, target, res);
    }

    public static void main(String[] args) {
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
}

package sim.util;

import java.util.*;

class Interval implements Comparable<Interval> {

    public int st, ed, max;
    public Interval left, right;

    public Interval(int st, int ed) {
        this.st = st;
        this.ed = ed;
        this.max = ed;
    }

    public String toString() {
        return "[" + this.st + ", " + this.ed + ")";
    }

    @Override
    public int compareTo(Interval other) {
        if (this.st < other.st)
            return -1;
        else if (this.st > other.st)
            return 1;

        return this.ed <= other.ed ? -1 : 1;
    }

    /**
    * overlapWith() and contains() assume the interval to be half-close half-open [st, ed)
    **/

    public boolean overlapWith(Interval other) {
        return !((this.st >= other.ed) || (this.ed <= other.st));
    }

    public boolean contains(int target) {
        return (this.st <= target) && (this.ed > target);
    }

}

public class IntervalTree {

    public Interval root;

    public void insert(int st, int ed) {
        this.insert(new Interval(st, ed));
    }

    public void insert(Interval target) {
        if (root == null)
            root = target;
        else
            insert(root, target);
    }

    private void insert(Interval curr, Interval target) {
        if (curr == null) {
            curr = target;
            return;
        }

        if (target.ed > curr.max) {
            curr.max = target.ed;
        }

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
        System.out.println("Interval Tree:");
        print(root);
        System.out.println();
    }

    private void print(Interval curr) {
        if (curr == null) {
            System.out.print("null ");
            return;
        }

        System.out.print(curr + " ");
        print(curr.left);
        print(curr.right);
    }

    public List<Interval> intersect(int st, int ed) {
        return this.intersect(new Interval(st, ed));
    }

    public List<Interval> intersect(Interval target) {
        List<Interval> res = new ArrayList<Interval>();
        intersect(root, target, res);
        return res;
    }

    private void intersect(Interval curr, Interval target, List<Interval> res) {
        if (curr == null)
            return;

        if (curr.overlapWith(target))
            res.add(curr);

        if ((curr.left != null) && (curr.left.max >= target.st))
            this.intersect(curr.left, target, res);

        this.intersect(curr.right, target, res);
    }

    public List<Interval> contains(int target) {
        List<Interval> res = new ArrayList<Interval>();
        contains(root, target, res);
        return res;
    }

    private void contains(Interval curr, int target, List<Interval> res) {
        if (curr == null)
            return;

        if (curr.contains(target))
            res.add(curr);

        if ((curr.left != null) && (curr.left.max >= target))
            this.contains(curr.left, target, res);

        this.contains(curr.right, target, res);
    }

    public static void main(String[] args) {
        List<Interval> res;
        IntervalTree t = new IntervalTree();

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
    }
}

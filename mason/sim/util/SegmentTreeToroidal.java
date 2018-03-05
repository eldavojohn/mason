package sim.util;

import java.util.*;

public class SegmentTreeToroidal extends SegmentTree {
	@Override
	public List<Segment> intersect(Segment target) {
		List<Segment> res = new ArrayList<Segment>();
		generate(target).forEach(seg -> intersect(root, seg, res));
		return res;
	}

	/**
	 *	Generate new segments based on the following rules (tab size=4)
	 *	'x' mean impossible case
	 *	----------------------------------------------------------------------------------------------------------------
	 *				 	|	<min		|	=min		|	min < ed < max		|	=max		|	>max
	 *	----------------------------------------------------------------------------------------------------------------
	 *				<min |gen(nst,ned)	|	(nst,max)	|	(nst,max),(min,ed)	|	(st,ed)		|	(st,ed)
	 *				=min |		x		|	()			|	(st,ed)				|	(st,ed)		|	(st,ed)
	 *	  min < st < max |		x		|		x		|	(st,ed)				|	(st,ed)		|	(st,max),(min,ned)
	 *			 	=max |		x		|		x		|		x				|	()			|	(min,ned)
	 *				>max |		x		|		x		|		x				|		x		|	gen(nst,ned)
	 *	----------------------------------------------------------------------------------------------------------------
	**/
	private List<Segment> generate(Segment orig) {
		double st = orig.st, ed = orig.ed;
		double min = root.min, max = root.max, len = max - min;
		List<Segment> ret = new ArrayList<Segment>();

		if (ed < min){
			double ned = conv(ed);
			return generate(new Segment(st + ned - ed, ned));
		} else if (st > max) {
			double nst = conv(st);
			return generate(new Segment(nst, ed + nst - st));
		}
		else if (ed == max || (min < ed && ed < max && min <= st && st < max) || (ed > max && st <= min))
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
	
	private double conv(final double val) {
		double len = root.max - root.min;
		if (val < root.min)
			return root.max - (root.min - val) % len;
		else if (val > root.max)
			return root.min + (val - root.max) % len;
		return val;
	}
	
	public static void main(String[] args) {
		List<Segment> res;
        SegmentTreeToroidal t = new SegmentTreeToroidal();

        t.insert(new Segment(2, 4, 0));
        t.insert(new Segment(3, 5, 1));
        t.insert(new Segment(3, 7, 2));
        t.insert(new Segment(6, 8, 3));
        t.insert(new Segment(9, 10, 4));
        t.insert(new Segment(5, 6, 5));
		
		List<Segment> testSegs = Arrays.asList(
			new Segment(-2, -1), new Segment(-2, 2), new Segment(-2, 5), new Segment(-2, 10), new Segment(-2, 100), 
			new Segment(2, 2), new Segment(2, 6), new Segment(2, 10), new Segment(2, 12),
			new Segment(4, 7), new Segment(4, 10), new Segment(4, 19),
			new Segment(10, 10), new Segment(10, 14), 
			new Segment(15, 25)
		);
		
		List<List<Integer>> expected = Arrays.asList(
			Arrays.asList(2, 3), Arrays.asList(2, 3, 4), Arrays.asList(0, 1, 2, 3, 4), Arrays.asList(0, 1, 2, 3, 4, 5), Arrays.asList(0, 1, 2, 3, 4, 5),
			Arrays.asList(), Arrays.asList(0, 1, 2, 5), Arrays.asList(0, 1, 2, 3, 4, 5), Arrays.asList(0, 1, 2, 3, 4, 5),
			Arrays.asList(1, 2, 3, 5), Arrays.asList(1, 2, 3, 4, 5), Arrays.asList(0, 1, 2, 3, 4, 5),
			Arrays.asList(), Arrays.asList(0, 1, 2, 5),
			Arrays.asList(0, 1, 2, 3, 4, 5)
		);
		
		Iterator<List<Integer>> eit = expected.iterator();
		Iterator<Segment> tit = testSegs.iterator();

		while (tit.hasNext() && eit.hasNext()) {
			Segment s = tit.next();
			res = t.intersect(s);
        	Set<Integer> s1 = new HashSet<Integer>(eit.next());
        	Set<Integer> s2 = new HashSet<Integer>(res.stream().mapToInt(x->x.pid).boxed().collect(java.util.stream.Collectors.toList()));
        	System.out.println("Got: " + s2 + " Want: " + s1);
        	assert s1.equals(s2);
		}
	}
}

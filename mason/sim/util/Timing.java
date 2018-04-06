package sim.util;

import java.util.HashMap;

import java.util.concurrent.TimeUnit;

public class Timing {

	private static Timing t;
	private static int movingAvgCapacity;
	private static HashMap<String, TimingStat> m;
	private static HashMap<String, Long> ts;

	private static NanoClock clock;

	public static void init(int capacity) {
		movingAvgCapacity = capacity;
		m = new HashMap<String, TimingStat>();
		ts = new HashMap<String, Long>();
		clock = new NanoClock() {
			public long nanoTime() {
				return System.nanoTime();
			}
		};
	}

	public static void start(String ... ids) {
		long curr = clock.nanoTime();
		for (String id : ids) {
			if (ts.getOrDefault(id, -1L) != -1L)
				throw new IllegalArgumentException("Timer for " + id + " is already started");
			ts.put(id, curr);
		}
	}

	public static void stop(String ... ids) {
		long curr = clock.nanoTime();
		for (String id : ids) {
			if (ts.getOrDefault(id, -1L) == -1L)
				throw new IllegalArgumentException("Timer for " + id + " is not started");
			if (!m.containsKey(id))
				m.put(id, new TimingStat(movingAvgCapacity));
			m.get(id).add((double)(curr - ts.get(id)));
			ts.put(id, -1L);
		}
	}

	public static void reset(String ... ids) {
		for (String id : ids) {
			m.put(id, new TimingStat(movingAvgCapacity));
			ts.put(id, -1L);
		}
	}

	public static TimingStat get(String s) {
		return m.get(s);
	}

	public static void main(String[] args) {
		init(3);
		FakeClock fakeClock = new FakeClock();
		clock = fakeClock;

		System.out.println("Name  \tCount \tOverall Mean \tMinimum \tMaximum \tOverall Stdev \tMoving Average \tMoving Stdev");
		fakeClock.val = 0L;

		start("Test1");
		fakeClock.val += 5000000L;
		stop("Test1");
		System.out.println("Test1\t" + get("Test1"));

		start("Test1");
		fakeClock.val += 6000000L;
		stop("Test1");
		System.out.println("Test1\t" + get("Test1"));

		start("Test1");
		fakeClock.val += 7000000L;
		stop("Test1");
		System.out.println("Test1\t" + get("Test1"));

		start("Test1");
		fakeClock.val += 3000000L;
		stop("Test1");
		System.out.println("Test1\t" + get("Test1"));

		start("Test1");
		fakeClock.val += 4000000L;
		stop("Test1");
		System.out.println("Test1\t" + get("Test1"));

		start("Test1", "Test2");
		fakeClock.val += 4000000L;
		stop("Test1");
		fakeClock.val += 4000000L;
		stop("Test2");
		System.out.println("Test1\t" + get("Test1"));
		System.out.println("Test2\t" + get("Test2"));

		reset("Test1");

		start("Test1", "Test2");
		fakeClock.val += 4000000L;
		stop("Test2", "Test1");
		System.out.println("Test1\t" + get("Test1"));
		System.out.println("Test2\t" + get("Test2"));

		start("Test1", "Test2");
		fakeClock.val += 100000000L;
		stop("Test2", "Test1");
		System.out.println("Test1\t" + get("Test1"));
		System.out.println("Test2\t" + get("Test2"));

		start("Test1", "Test2");
		fakeClock.val += 40000000L;
		stop("Test2", "Test1");
		System.out.println("Test1\t" + get("Test1"));
		System.out.println("Test2\t" + get("Test2"));
	}

	private static class FakeClock implements NanoClock {
		public long val;

		public long nanoTime() {
			return val;
		}
	}

	private interface NanoClock {
		long nanoTime();
	}
}

class TimingStat {

	long cnt;
	double avg, min, max, var;
	MovingAverage mav;
	int cap;

	final long conv;

	public TimingStat(int cap) {
		this(cap, TimeUnit.MILLISECONDS);
	}

	public TimingStat(int cap, TimeUnit u) {
		this.cap = cap;
		this.conv = TimeUnit.NANOSECONDS.convert(1L, u);
		reset();
	}

	public void add(double val) {
		min = Math.min(min, val);
		max = Math.max(max, val);

		double avg_old = avg;
		avg += (val - avg) / ++cnt;
		var += (val - avg_old) * (val - avg);

		mav.next(val);
	}

	public void reset() {
		mav = new MovingAverage(cap);
		cnt = 0;
		min = Double.MAX_VALUE;
		max = 0;
		avg = 0;
		var = 0;
	}

	public long getCount() {
		return cnt;
	}

	public double getMean() {
		return avg / conv;
	}

	public double getMin() {
		return min / conv;
	}

	public double getMax() {
		return max / conv;
	}

	public double getStdev() {
		if (cnt > 1)
			return Math.sqrt(var / (cnt - 1)) / conv;
		return 0;
	}

	public double getMovingAverage() {
		return mav.average() / conv;
	}

	public double getMovingStdev() {
		return mav.stdev() / conv;
	}

	public String toString() {
		return String.format("%d\t%f\t%f\t%f\t%f\t%f\t%f", getCount(), getMean(), getMin(), getMax(), getStdev(), getMovingAverage(), getMovingStdev());
	}
}
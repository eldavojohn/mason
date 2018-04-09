package sim.util;

import java.util.HashMap;
import java.util.NoSuchElementException;

public class Timing {

	// Used by load balancer to determine the workload on each node
	public static final String LB_RUNTIME = "MASON_LOAD_BALANCING_RUNTIME";
	// Used by load balancer to determine the overhead for each load balancing
	public static final String LB_OVERHEAD = "MASON_LOAD_BALANCING_OVERHEAD";

	public static final String MPI_SYNC_OVERHEAD = "MASON_MPI_SYNC_OVERHEAD";

	private static int cap;
	private static HashMap<String, TimingStat> m;
	private static NanoClock clock;

	private static class FakeClock implements NanoClock {
		public long val;
		public long nanoTime() { return val; }
	}

	private interface NanoClock {
		long nanoTime();
	}

	// TODO allow multiple init? or do we need to init at all?
	public static void init(int capacity) {
		cap = capacity;
		m = new HashMap<String, TimingStat>();
		clock = new NanoClock() { public long nanoTime() { return System.nanoTime(); } };
	}

	public static void initMetrics(String ... ids) {
		for (String id : ids)
			if (m.containsKey(id))
				throw new IllegalArgumentException("Timer for " + id + " already exists");
			else
				m.put(id, new TimingStat(cap));
	}

	public static void start(String ... ids) {
		for (String id : ids)
			if (!m.containsKey(id))
				throw new NoSuchElementException("Timer for " + id + " does not exist");
			else
				m.get(id).start(clock.nanoTime());
	}

	public static void stop(String ... ids) {
		for (String id : ids)
			if (!m.containsKey(id))
				throw new NoSuchElementException("Timer for " + id + " does not exist");
			else
				m.get(id).stop(clock.nanoTime());
	}

	public static void reset(String ... ids) {
		for (String id : ids)
			if (!m.containsKey(id))
				throw new NoSuchElementException("Timer for " + id + " does not exist");
			else
				m.get(id).reset();
	}

	public static TimingStat get(String id) {
		if (m == null || !m.containsKey(id))
			throw new NoSuchElementException("Timer for " + id + " does not exist or Timing not init");
		return m.get(id);
	}

	public static void main(String[] args) {
		init(3);
		initMetrics("Test1", "Test2");
		
		FakeClock fakeClock = new FakeClock();
		clock = fakeClock;

		System.out.println("Name  \tCount \tMinimum \tMaximum \tOverall Mean \tOverall Stdev \tMoving Average \tMoving Stdev \tUnit");
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
}
package sim.field;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import sim.util.IntHyperRect;
import sim.util.GraphColoring;
import sim.util.MPITest;
import sim.util.Timing;
import sim.field.grid.NDoubleGrid2D;

import mpi.*;

public class LoadBalancer {

	DNonUniformPartition p;
	GraphColoring gc;
	int interval;

	// Offsets per adjustment on each dimension
	int[] offsets;

	public LoadBalancer(int[] offsets, int interval) {
		this.p = DNonUniformPartition.getPartitionScheme();
		this.offsets = offsets;
		this.interval = interval;
		this.gc = new GraphColoring(p);

		gc.color();

		p.registerPostCommit(new Runnable() {
			public void run() {
				gc.color();
			}
		});
	}

	// Get all the neighbor ids and then
	// filter out those who don't align with this partition
	// Return the pids grouped by the dimension
	private int[][] getAvailNeighborIds() {
		int[][] ret = new int[p.nd][];
		IntHyperRect self = p.getPartition();

		for (int d = 0; d < p.nd; d++) {
			final int fd = d;
			IntStream s = IntStream.concat(self.br.c[d] == p.size[d] ? IntStream.empty() : Arrays.stream(p.getNeighborIdsShift(d, 0)),
			                               self.ul.c[d] == 0 ? IntStream.empty() : Arrays.stream(p.getNeighborIdsShift(d, -1)));
			ret[d] = s.filter(i -> p.getPartition(i).isAligned(self, fd)).toArray();
		}

		return ret;
	}

	private boolean shouldBalance(int step) {
		if (interval < 0)
			return false;

		return step % (gc.numColors + interval) < gc.numColors;
	}

	private boolean isMyTurn(int step) {
		return step % (gc.numColors + interval) == gc.myColor;
	}

	private int _balance(int step, HashMap<Integer, Double> rts, double overhead) throws MPIException, IOException {
		if (!shouldBalance(step))
			return 0;

		// Buffers to hold incoming actions
		int[] actions = new int[p.np * BalanceAction.size];
		int count = 0;

		BalanceAction myAction = new BalanceAction();

		if (isMyTurn(step)) {
			int dim = 0, myPid = p.getPid(), target = myPid, offset = 0;
			double maxDelta = 0, myRt = rts.get(myPid);
			int[] size = p.getPartition().getSize();
			int[][] avail = getAvailNeighborIds();

			for ( int d = 0; d < p.nd; d++) {
				for (int t : avail[d]) {
					double delta = (myRt - rts.get(t)) * offsets[d] / size[d];
					if (Math.abs(delta) > maxDelta) {
						maxDelta = Math.abs(delta);
						dim = d;
						target = t;
						offset = delta > 0 ? -offsets[d] : offsets[d];
					}
				}
			}

			// balance only if the delta is large enough
			if (maxDelta * (gc.numColors + interval) > overhead)
				myAction = new BalanceAction(myPid, target, dim, offset);
		}

		// TODO maybe use DObjectMigrator here?
		myAction.writeToBuf(actions, p.pid);

		//MPITest.execInOrder(i -> System.out.println(String.format("[%d] %s", p.pid, myAction.toString())), 0);

		// Exchange actions
		p.getCommunicator().allGather(actions, BalanceAction.size, MPI.INT);

		// Everyone commits the changes to their local partition scheme
		for (BalanceAction a : BalanceAction.toActions(actions))
			count += a.applyToPartition(p);

		p.commit();

		return count;
	}

	public int balance(int step, HashMap<Integer, Double> rts) throws MPIException, IOException {
		Timing.start(Timing.LB_OVERHEAD);
		int count = _balance(step, rts, Timing.get(Timing.LB_OVERHEAD).getMovingAverage());
		Timing.stop(Timing.LB_OVERHEAD);
		return count;
	}

	public static void main(String args[]) throws MPIException, InterruptedException, IOException {
		int[] size = new int[] {10, 10};
		int[] aoi = new int[] {1, 1};

		MPI.Init(args);

		DNonUniformPartition p = DNonUniformPartition.getPartitionScheme(size, true);
		assert p.np == 4;
		p.initUniformly(null);
		p.commit();

		NDoubleGrid2D f = new NDoubleGrid2D(p, aoi, p.pid);
		f.sync();
		MPITest.execOnlyIn(0, i -> System.out.println("Initial field"));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		LoadBalancer lb = new LoadBalancer(aoi, 0);
		HashMap<Integer, Double> rts = new HashMap<Integer, Double>();
		rts.put(0, 100.0);
		rts.put(1, 100.0);
		rts.put(2, 100.0);
		rts.put(3, 100.0);

		final int res1 = lb._balance(0, rts, 10);
		MPITest.execOnlyIn(0, i -> System.out.println("Load Balancing #1: " + res1));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		rts.put(0, 1100.0);
		rts.put(2, 100.0);

		final int res2 = lb._balance(1, rts, 10);
		MPITest.execOnlyIn(0, i -> System.out.println("Load Balancing #2: " + res2));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		final int res3 = lb._balance(2, rts, 400);
		MPITest.execOnlyIn(0, i -> System.out.println("Load Balancing #3: " + res3));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		rts.put(2, 10.0);

		final int res4 = lb._balance(3, rts, 71);
		MPITest.execOnlyIn(0, i -> System.out.println("Load Balancing #4: " + res4));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		MPI.Finalize();
	}
}

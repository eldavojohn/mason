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

	//Comm comm;

	// Offsets per adjustment on each dimension
	int[] offsets;

	public LoadBalancer(int[] offsets, int interval) {
		this.p = DNonUniformPartition.getPartitionScheme();
		this.offsets = offsets;
		this.interval = interval;
		this.gc = new GraphColoring(p);

		reload();

		p.registerPostCommit(new Runnable() {
			public void run() {
				gc.color();
			}
		});
	}

	private void reload() {
		gc.color();
		// try {
		// 	comm = MPI.COMM_WORLD.split(gc.myColor, p.getPid());
		// } catch (MPIException e) {
		// 	e.printStackTrace();
		// 	System.exit(-1);
		// }
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

	private HashMap<Integer, Double> collectRuntimes(double myrt) throws MPIException {
		HashMap<Integer, Double> nrts = new HashMap<Integer, Double>();

		int[] neighbors = p.getNeighborIds();
		double[] sendBuf = new double[] {myrt};
		double[] recvBuf = new double[neighbors.length];

		p.getCommunicator().neighborAllGather(sendBuf, 1, MPI.DOUBLE, recvBuf, 1, MPI.DOUBLE);

		IntStream.range(0, neighbors.length).forEach(i -> nrts.put(neighbors[i], recvBuf[i]));

		return nrts;
	}

	private int doBalance(int step, double myrt, double overhead) throws MPIException, IOException {
		// Collect runtimes of all the neighbors
		HashMap<Integer, Double> nrts = collectRuntimes(myrt);

		// Buffers to hold incoming actions
		int[] actions = new int[p.np * BalanceAction.size];
		int count = 0;

		BalanceAction myAction = new BalanceAction();

		if (isMyTurn(step)) {
			int dim = 0, target = 0, offset = 0;
			int[] size = p.getPartition().getSize();
			int[][] avail = getAvailNeighborIds();
			double maxDelta = 0;

			for ( int d = 0; d < p.nd; d++) {
				for (int t : avail[d]) {
					double delta = (myrt - nrts.get(t)) * offsets[d] / size[d];
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
				myAction = new BalanceAction(p.getPid(), target, dim, offset);
		}

		// TODO maybe use DObjectMigrator here?
		myAction.writeToBuf(actions, p.pid);

		// final BalanceAction fba = myAction;
		// MPITest.execInOrder(i -> System.out.println(String.format("[%d] %s", p.pid, fba.toString())), 0);

		// Exchange actions
		p.getCommunicator().allGather(actions, BalanceAction.size, MPI.INT);

		// Everyone commits the changes to their local partition scheme
		for (BalanceAction a : BalanceAction.toActions(actions))
			count += a.applyToPartition(p);

		if (count > 0)
			p.commit();

		return count;
	}

	public int balance(int step) throws MPIException, IOException {
		if (!shouldBalance(step))
			return 0;

		double runtime;
		try {
			runtime = Timing.get(Timing.LB_RUNTIME).getMovingAverage();
		} catch (NoSuchElementException e) {
			return 0;
		}

		Timing.start(Timing.LB_OVERHEAD);

		int count = doBalance(
		                step,
		                runtime,
		                //Timing.get(Timing.LB_OVERHEAD).getMovingAverage()
		                0.0
		            );

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

		int pid = p.getPid();
		NDoubleGrid2D f = new NDoubleGrid2D(p, aoi, pid);
		f.sync();

		MPITest.execOnlyIn(0, i -> System.out.println("Initial field"));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		LoadBalancer lb = new LoadBalancer(aoi, 0);

		double[] rts = new double[] {100, 100, 100, 100};

		final int res1 = lb.doBalance(0, rts[pid], 10);
		MPITest.execOnlyIn(0, i -> System.out.println("Load Balancing #1: " + res1));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		rts[0] = 1100.0;
		rts[2] = 100.0;

		final int res2 = lb.doBalance(1, rts[pid], 10);
		MPITest.execOnlyIn(0, i -> System.out.println("Load Balancing #2: " + res2));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		final int res3 = lb.doBalance(2, rts[pid], 400);
		MPITest.execOnlyIn(0, i -> System.out.println("Load Balancing #3: " + res3));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		rts[2] = 10.0;

		final int res4 = lb.doBalance(3, rts[pid], 71);
		MPITest.execOnlyIn(0, i -> System.out.println("Load Balancing #4: " + res4));
		MPITest.execInOrder(i -> System.out.println(f), 500);

		MPI.Finalize();
	}
}

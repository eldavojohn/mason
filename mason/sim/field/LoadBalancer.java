package sim.field;

import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import sim.util.IntHyperRect;
import sim.util.GraphColoring;
import sim.util.MPITest;
import sim.field.grid.NDoubleGrid2D;

import mpi.*;

public class LoadBalancer {

	DNonUniformPartition p;
	HaloField f;

	GraphColoring gc;

	double threshold;
	//int interval;

	// Use aoi as the offset to adjust partitions
	int[] aoi;

	public int count;

	public LoadBalancer(DNonUniformPartition p, HaloField f, int[] aoi, double threshold) {
		this.p = p;
		this.f = f;
		this.aoi = aoi;
		this.threshold = threshold;
		//this.interval = interval;

		this.gc = new GraphColoring(p);
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
		gc.color();
		if (step % gc.numColors == gc.myColor)
			return true;
		return false;
	}

	private BalanceAction getAction(int[][] avail) {
		HashMap<Integer, Double> m = f.getRuntimes();

		int dim = 0, myPid = p.getPid(), target = myPid, offset = 0;
		double myRt = m.get(myPid), maxDelta = 0;
		int[] size = p.getPartition().getSize();

		for ( int d = 0; d < p.nd; d++) {
			for (int t : avail[d]) {
				double delta = (myRt - m.get(t)) * aoi[d] / size[d];
				if (Math.abs(delta) > maxDelta) {
					maxDelta = Math.abs(delta);
					dim = d;
					target = t;
					offset = delta > 0 ? -1 : 1;
				}
			}
		}

		// do not balance if the delta is too small
		if (maxDelta < threshold)
			offset = 0;

		if (offset != 0 && myPid != target)
			count += 1;

		return new BalanceAction(myPid, target, dim, offset * aoi[dim]);
	}

	// Randomly choosing targets
	// TODO use the random number generator in MASON
	private BalanceAction getRandomAction(int[][] avail) {
		java.util.Random r = new java.util.Random();

		int[] nonEmptyIdxs = IntStream.range(0, p.nd).filter(x -> avail[x].length > 0).toArray();
		if (nonEmptyIdxs == null)
			return new BalanceAction();

		int dim = nonEmptyIdxs[r.nextInt(nonEmptyIdxs.length)];
		int target = avail[dim][r.nextInt(avail[dim].length)];
		int offset = r.nextBoolean() ? aoi[dim] : -aoi[dim];

		return new BalanceAction(p.pid, target, dim, offset);
	}

	private BalanceAction generateAction(int step) {
		if (!shouldBalance(step))
			return new BalanceAction();

		return getAction(getAvailNeighborIds());
	}

	public int balance(int step) throws MPIException, IOException {
		// Buffers to hold incoming actions
		int[] actions = new int[p.np * BalanceAction.size];
		int count = 0;

		// Generate own load balancing action
		BalanceAction myAction = generateAction(step);
		myAction.writeToBuf(actions, p.pid);

		//MPITest.execInOrder(i -> System.out.println(String.format("[%d] %s", p.pid, myAction.toString())), 0);

		// Exchange actions
		p.getCommunicator().allGather(actions, BalanceAction.size, MPI.INT);

		//MPITest.execInOrder(i -> System.out.println(String.format("[%d] %s", p.pid, Arrays.toString(actions))), 0);

		// Everyone commits the changes to their local partition scheme
		for (BalanceAction a : BalanceAction.toActions(actions))
			count += a.applyToPartition(p);

		p.setMPITopo();

		// Sync HaloField data
		// f.reload();
		// f.sync();

		return count;
	}

	public static void main(String args[]) throws MPIException, InterruptedException, IOException {
		int[] size = new int[] {10, 10};
		int[] aoi = new int[] {1, 1};

		MPI.Init(args);

		DNonUniformPartition p = new DNonUniformPartition(size, true);
		assert p.np == 4;
		p.initUniformly(null);
		p.setMPITopo();

		FakeField hf = new FakeField(p, aoi, p.pid);
		hf.sync();

		MPITest.execInOrder(i -> System.out.println(hf), 500);

		LoadBalancer lb = new LoadBalancer(p, hf, aoi, 0);

		lb.balance(0);
		hf.reload();
		hf.sync();
		MPITest.execInOrder(i -> System.out.println(hf), 500);
		hf.setRuntimes(new double[] {200 , 100, 100, 50});

		lb.balance(1);
		hf.reload();
		hf.sync();
		MPITest.execInOrder(i -> System.out.println(hf), 500);
		//hf.setRuntimes(new double[]{ , , , });

		lb.balance(2);
		hf.reload();
		hf.sync();
		MPITest.execInOrder(i -> System.out.println(hf), 500);
		//hf.setRuntimes(new double[]{ , , , });

		lb.balance(3);
		hf.reload();
		hf.sync();
		MPITest.execInOrder(i -> System.out.println(hf), 500);

		MPI.Finalize();
	}

	static class FakeField extends NDoubleGrid2D {
		public HashMap<Integer, Double> rt;

		public FakeField(DPartition ps, int[] aoi, double initVal) {
			super(ps, aoi, initVal);
			rt = new HashMap<Integer, Double>();
			for (int i = 0; i < ps.getNumProc(); i++)
				rt.put(i, 100.0);
		}

		public void setRuntimes(double[] rts) {
			for (int i = 0; i < ps.getNumProc(); i++)
				rt.put(i, rts[i]);
		}

		@Override
		public HashMap<Integer, Double> getRuntimes() {
			HashMap<Integer, Double> ret = new HashMap<Integer, Double>();

			ret.put(ps.getPid(), rt.get(ps.getPid()));
			Arrays.stream(neighbors).forEach(x -> ret.put(x.pid, rt.get(x.pid)));

			return ret;
		}
	}
}

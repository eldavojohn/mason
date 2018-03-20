package sim.field;

import java.util.Arrays;
import java.util.stream.IntStream;

import sim.util.IntHyperRect;

import mpi.*;

public class LoadBalancer {

	DNonUniformPartition p;
	HaloField f;

	// Use aoi as the offset to adjust partitions
	int[] aoi;

	public LoadBalancer(DNonUniformPartition p, HaloField f, int[] aoi) {
		this.p = p;
		this.f = f;
		this.aoi = aoi;
	}

	// Get all the neighbor ids and then
	// filter out those who don't align with this partition
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

	// TODO use graph coloring to allow multiple nodes to perform balancing at the same time
	private boolean shouldBalance(int step) {
		return step % p.np == p.pid ;
	}

	// Now it is just randomly choosing targets
	// TODO choose target to minimize the variance of runtime between this node and its available neighbors
	// TODO use the random number generator in MASON
	private BalanceAction generateAction(int step) {
		if (!shouldBalance(step))
			return new BalanceAction();

		int[][] avail = getAvailNeighborIds();

		java.util.Random r = new java.util.Random();

		int[] nonEmptyIdxs = IntStream.range(0, p.nd).filter(x -> avail[x].length > 0).toArray();
		if (nonEmptyIdxs == null)
			return new BalanceAction();

		int dim = nonEmptyIdxs[r.nextInt(nonEmptyIdxs.length)];
		int target = avail[dim][r.nextInt(avail[dim].length)];
		int offset = r.nextBoolean() ? aoi[dim] : -aoi[dim];

		return new BalanceAction(p.pid, target, dim, offset);
	}

	public void balance(int step) throws MPIException {
		// Buffers to hold incoming actions
		int[] actions = new int[p.np * BalanceAction.size];

		// Generate own load balancing action
		BalanceAction myAction = generateAction(step);
		myAction.writeToBuf(actions, p.pid);
		System.out.println(String.format("[%d] %s", p.pid, myAction.toString()));

		// Exchange actions
		p.getCommunicator().allGather(actions, BalanceAction.size, MPI.INT);
		System.out.println(String.format("[%d] %s", p.pid, Arrays.toString(actions)));

		// Everyone commits the changes to their local partition scheme
		for (BalanceAction a : BalanceAction.toActions(actions))
			a.applyToPartition(p);
		p.setMPITopo();

		// Sync HaloField data
		f.reload();
		f.sync();
	}

	public static void main(String args[]) throws MPIException, InterruptedException {
		int[] size = new int[] {10, 10};
		int[] aoi = new int[] {1, 1};

		MPI.Init(args);

		DNonUniformPartition p = new DNonUniformPartition(size);
		assert p.np == 4;
		p.initUniformly(null);
		p.setMPITopo();

		HaloField hf = new HaloField(p, aoi, p.pid);
		hf.sync();

		LoadBalancer lb = new LoadBalancer(p, hf, aoi);
		lb.printHaloField();
		for (int i = 0; i < 5; i++) {
			lb.balance(i);
			lb.printHaloField();
		}

		MPI.Finalize();
	}

	private void printHaloField() throws MPIException, InterruptedException {
		MPI.COMM_WORLD.barrier();

		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);

		System.out.println("PID " + p.pid + " data: ");
		int w = p.getPartition().getSize()[0] + 2 * aoi[0];
		int h = p.getPartition().getSize()[1] + 2 * aoi[1];
		for (int i = 0; i < w; i++) {
			if (i == 1 || i == w - 1)
				System.out.println("");
			for (int j = 0; j < h; j++) {
				if (j == 1 || j == h - 1)
					System.out.printf("   \t");
				System.out.printf("%.1f\t", f.field[i * h + j]);
			}
			System.out.printf("\n");
		}
		System.out.printf("\n");
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);

		MPI.COMM_WORLD.barrier();
	}
}
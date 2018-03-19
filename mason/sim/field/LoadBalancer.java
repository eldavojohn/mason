package sim.field;

import java.util.Random;
import java.util.Arrays;
import java.util.stream.*;

import sim.util.*;

import mpi.*;

public class LoadBalancer {

	DNonUniformPartition p;
	HaloField f;

	public LoadBalancer(DNonUniformPartition p, HaloField f) {
		this.p = p;
		this.f = f;
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
	private int[] chooseTarget(int[][] avail) {
		Random r = new Random();
		int[] nonEmpty = IntStream.range(0, p.nd).filter(x -> avail[x].length > 0).toArray();
		if (nonEmpty == null)
			return null;
		for (int x : nonEmpty)
			System.out.println("Dim " + x + " " + Arrays.toString(avail[x]));

		int dim = nonEmpty[r.nextInt(nonEmpty.length)];
		int target = avail[dim][r.nextInt(avail[dim].length)];
		int dir = r.nextBoolean() ? 1 : -1;

		return new int[] {p.pid, target, dim, dir};
	}

	public void balance(int step) throws MPIException {
		int[] actions = new int[p.np * 4];
		int[] myaction = new int[4];

		if (shouldBalance(step)) {
			int[][] avail = getAvailNeighborIds();
			myaction = chooseTarget(avail);
		}

		// Exchange partition scheme changes
		p.getCommunicator().allGather(myaction, 4, MPI.INT, actions, 4, MPI.INT);

		System.out.println(String.format("[%d] %s", p.pid, Arrays.toString(actions)));

		// Everyone commits the changes to their local partition scheme
		for (int i = 0; i < actions.length; i += 4) {
			IntHyperRect p1 = p.getPartition(actions[i]);
			IntHyperRect p2 = p.getPartition(actions[i + 1]);
			int dim = actions[i + 2];
			int dir = actions[i + 3];
			// let from always be the partition that is "above" to
			IntHyperRect from = p1.ul.c[dim] < p2.ul.c[dim] ? p1 : p2;
			IntHyperRect to = p1.ul.c[dim] < p2.ul.c[dim] ? p2 : p1;
			IntHyperRect newFrom = null, newTo = null;

			// dir =  1 = expand from and shrink to
			// dir = -1 = shrink from and expand to
			// dir =  0 = no change
			if (dir == 0)
				continue;
			newFrom = from.resize(dim, 1, dir * f.aoi[dim]);
			newTo = to.resize(dim, -1, - dir * f.aoi[dim]);

			System.out.println(String.format("from [%d] %s -> %s", p.pid, from.toString(), newFrom.toString()));
			System.out.println(String.format("  to [%d] %s -> %s", p.pid, to.toString(), newTo.toString()));

			p.updatePartition(newFrom);
			p.updatePartition(newTo);
		}
		p.setMPITopo();
		f.reload();

		// Sync data
		f.sync();
	}

	public static void main(String args[]) throws MPIException, InterruptedException {
		int[] size = new int[] {10, 10};
		int[] aoi = new int[] {1, 1};
		int w, h;

		MPI.Init(args);

		DNonUniformPartition p = new DNonUniformPartition(size);

		assert p.np == 4;

		p.initUniformly(null);
		p.setMPITopo();

		HaloField hf = new HaloField(p, aoi, p.pid);
		hf.sync();

		LoadBalancer lb = new LoadBalancer(p, hf);

		MPI.COMM_WORLD.barrier();
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		w = p.getPartition().getSize()[0] + 2 * aoi[0];
		h = p.getPartition().getSize()[1] + 2 * aoi[1];
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", hf.field[i * h + j]);
			System.out.printf("\n");
		}
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);
		MPI.COMM_WORLD.barrier();

		lb.balance(0);

		MPI.COMM_WORLD.barrier();
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		w = p.getPartition().getSize()[0] + 2 * aoi[0];
		h = p.getPartition().getSize()[1] + 2 * aoi[1];
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", hf.field[i * h + j]);
			System.out.printf("\n");
		}
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);
		MPI.COMM_WORLD.barrier();

		lb.balance(1);

		MPI.COMM_WORLD.barrier();
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		w = p.getPartition().getSize()[0] + 2 * aoi[0];
		h = p.getPartition().getSize()[1] + 2 * aoi[1];
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", hf.field[i * h + j]);
			System.out.printf("\n");
		}
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);
		MPI.COMM_WORLD.barrier();

		lb.balance(2);

		MPI.COMM_WORLD.barrier();
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		w = p.getPartition().getSize()[0] + 2 * aoi[0];
		h = p.getPartition().getSize()[1] + 2 * aoi[1];
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", hf.field[i * h + j]);
			System.out.printf("\n");
		}
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);
		MPI.COMM_WORLD.barrier();

		lb.balance(3);

		MPI.COMM_WORLD.barrier();
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		w = p.getPartition().getSize()[0] + 2 * aoi[0];
		h = p.getPartition().getSize()[1] + 2 * aoi[1];
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", hf.field[i * h + j]);
			System.out.printf("\n");
		}
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);
		MPI.COMM_WORLD.barrier();

		lb.balance(4);

		MPI.COMM_WORLD.barrier();
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.pid);
		System.out.println("PID " + p.pid + " data: ");
		w = p.getPartition().getSize()[0] + 2 * aoi[0];
		h = p.getPartition().getSize()[1] + 2 * aoi[1];
		for (int i = 0; i < w; i++) {
			for (int j = 0; j < h; j++)
				System.out.printf("%.1f\t", hf.field[i * h + j]);
			System.out.printf("\n");
		}
		java.util.concurrent.TimeUnit.SECONDS.sleep(p.np - p.pid);
		MPI.COMM_WORLD.barrier();

		MPI.Finalize();
	}
}
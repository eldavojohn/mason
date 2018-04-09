package sim.field;

import java.util.Arrays;
import java.util.ArrayList;
import mpi.*;

import sim.util.IntHyperRect;
import sim.util.IntPoint;

public abstract class DPartition {

	public int pid, np, nd;
	public int[] size;
	boolean isToroidal;
	public Comm comm;

	ArrayList<Runnable> preCallbacks, postCallbacks;

	DPartition(int[] size, boolean isToroidal) {
		this.nd = size.length;
		this.size = Arrays.copyOf(size, nd);
		this.isToroidal = isToroidal;

		preCallbacks = new ArrayList<Runnable>();
		postCallbacks = new ArrayList<Runnable>();
	}

	public int getPid() {
		return pid;
	}

	public int getNumProc() {
		return np;
	}

	public int getNumDim() {
		return nd;
	}

	public boolean isToroidal() {
		return isToroidal;
	}

	public Comm getCommunicator() {
		return comm;
	}

	public int[] getFieldSize() {
		return Arrays.copyOf(size, nd);
	}

	public IntHyperRect getField() {
		return new IntHyperRect(size);
	}

	public abstract IntHyperRect getPartition();
	public abstract IntHyperRect getPartition(int pid);

	public abstract int[] getNeighborIds();
	public abstract int[][] getNeighborIdsInOrder();

	public abstract int toPartitionId(IntPoint p);

	// TODO let other classes who depend on the partition scheme to register proper actions when partiton changes
	public void registerPreCommit(Runnable r) {
		preCallbacks.add(r);
	}

	public void registerPostCommit(Runnable r) {
		postCallbacks.add(r);
	}
}
package sim.field;

import java.util.Arrays;
import mpi.*;

import sim.util.IntHyperRect;
import sim.util.IntPoint;

public abstract class DPartition {

	public int pid, np, nd;
	public int[] size;
	boolean isToroidal;
	public Comm comm;

	public DPartition(int[] size, boolean isToroidal) {
		this.nd = size.length;
		this.size = Arrays.copyOf(size, nd);
		this.isToroidal = isToroidal;
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
}
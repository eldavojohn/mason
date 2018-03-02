package sim.field;

import mpi.*;

import sim.util.IntHyperRect;

public interface DPartition {
	int getNumDim();
	Comm getCommunicator();
	int[] getFieldSize();
	IntHyperRect getPartition();
	IntHyperRect getPartition(int pid);
	boolean isToroidal();
	boolean isExtendedNeighborhood();
	int[][] getNeighborIdsInOrder();
}
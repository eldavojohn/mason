package sim.field;

import java.io.*;
import java.util.*;

import sim.util.*;
import ec.util.*;

import mpi.*;

public abstract class ObjectMigrator {

	DPartition p;
	HashMap<Integer, Neighbor> m;

	public ObjectMigrator(DPartition p) {
		this.p = p;
		this.m = new HashMap<Integer, Neighbor>();
		for (int id : p.getNeighborIds())
			m.put(id, new Neighbor(id));
	}

	public abstract void schedSend(Object obj, int dst);

	public abstract void sync();

	protected abstract byte[] writeObject(Object obj);

	protected abstract Object recvObject(byte[] buf, int pos);

	protected abstract void onRecvObj(Object obj);

	class Neighbor {
		public int pid;

		public Neighbor(int pid) {
			this.pid = pid;
		}
	}
}
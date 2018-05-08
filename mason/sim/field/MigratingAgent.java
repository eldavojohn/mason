package sim.field;

import java.io.Serializable;

import sim.util.Double2D;

public class MigratingAgent implements Serializable
{
	public int destination;
	public boolean migrate;
	public Object wrappedAgent;
	public Double2D loc;
	public int identityHashcode;

	public MigratingAgent(final int dst, final Object agent, final Double2D loc, final boolean migrate) {
		this.destination = dst;
		this.wrappedAgent = agent;
		this.migrate = migrate;
		this.loc = loc;
	}

	public MigratingAgent(final int dst, final Object agent, final Double2D loc) {
		this(dst, agent, loc, false);
	}
}

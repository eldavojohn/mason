package sim.field.continuous;

import sim.field.MigratingAgent;
import sim.util.Double2D;

public class DContinuous2DAgent extends MigratingAgent
{
	public Double2D loc;

	public DContinuous2DAgent(final int dst, final Object agent, final Double2D loc, final boolean migrate) {
		this.destination = dst;
		this.wrappedAgent = agent;
		this.migrate = migrate;
		this.loc = loc;
	}

	public DContinuous2DAgent(final int dst, final Object agent, final Double2D loc) {
		this(dst, agent, loc, false);
	}
}

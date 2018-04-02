package sim.field;

import java.io.Serializable;

public abstract class MigratingAgent implements Serializable
{
	public int destination;
	public boolean migrate;
	public Object wrappedAgent;
}

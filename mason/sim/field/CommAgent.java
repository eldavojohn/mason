package sim.field;

import java.io.ObjectInputStream;

import sim.field.DObjectMigrator.AgentOutputStream;

public class CommAgent implements Cloneable
{
	public boolean migrate;
	
	public void writePrimitiveTypeData(AgentOutputStream out)
	{
		throw new UnsupportedOperationException();
	}
	
	public void readPrimitiveTypeData(ObjectInputStream in)
	{
		throw new UnsupportedOperationException();
	}
	
	public Object clone()
	{
		throw new UnsupportedOperationException();
	}
}

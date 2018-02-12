package sim.field.continuous;

import java.io.IOException;
import java.io.ObjectInputStream;

import sim.engine.SimState;
import sim.engine.Steppable;
import sim.field.CommAgent;
import sim.field.DObjectMigrator.AgentOutputStream;
import sim.util.Double2D;

public class DContinuous2DRawTypeTestObject extends CommAgent implements Steppable
{
	public int id;
	public Double2D loc;

	public DContinuous2DRawTypeTestObject(int id, Double2D loc)
	{
		this.id = id;
		this.loc = loc;
	}

	public String toString()
	{
		return String.format("Object %d at location [%g, %g]", id, loc.x, loc.y);
	}

	public void step(SimState state)
	{
		return;
	}

	public void writePrimitiveTypeData(AgentOutputStream out)
	{
		try
		{
			out.os.writeInt(id);
			out.os.writeDouble(loc.x);
			out.os.writeDouble(loc.y);
		} catch (IOException e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void readPrimitiveTypeData(ObjectInputStream in)
	{
		try
		{
			this.id = in.readInt();
			double x = in.readDouble();
			double y = in.readDouble();
			this.loc = new Double2D(x, y);
		}
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public Object clone()
	{
		DContinuous2DRawTypeTestObject obj =  new DContinuous2DRawTypeTestObject(id, new Double2D(loc.x, loc.y));
		return obj;
	}

}

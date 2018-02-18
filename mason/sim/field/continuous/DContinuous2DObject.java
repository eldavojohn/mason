package sim.field.continuous;

import java.io.Serializable;

import sim.util.*;

public class DContinuous2DObject implements Serializable {
	public Object obj;
	public boolean migrate;
	public Double2D loc;

	public DContinuous2DObject(final Object obj, final Double2D loc, final boolean migrate) {
		this.obj = obj;
		this.migrate = migrate;
		this.loc = loc;
	}

	public DContinuous2DObject(final Object obj, final Double2D loc) {
		this(obj, loc, false);
	}
}
/*
 * Copyright 2011 by Mark Coletti, Keith Sullivan, Sean Luke, and
 * George Mason University Mason University Licensed under the Academic
 * Free License version 3.0
 *
 * See the file "LICENSE" for more information
 *
 * $Id: Agent.java 846 2013-01-08 21:47:51Z mcoletti $
 */
package sim.app.geo.communication;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.linearref.LengthIndexedLine;
import com.vividsolutions.jts.planargraph.DirectedEdgeStar;
import com.vividsolutions.jts.planargraph.Node;

import mpi.MPIException;
import sim.engine.SimState;
import sim.engine.Steppable;
import sim.util.DoublePoint;
import sim.util.geo.GeomPlanarGraphDirectedEdge;
import sim.util.geo.GeomPlanarGraphEdge;
import sim.util.geo.MasonGeometry;
import sim.util.geo.PointMoveTo;



/**
 * Our simple agent for the CampusWorld GeoMASON example. The agent randomly
 * wanders around the campus walkways. When the agent reaches an intersection,
 * it continues in a random direction.
 *
 */
public class DAgent implements Steppable
{

    private static final long serialVersionUID = 1L;
    
    public DoublePoint position;
    public int[] content;

    
    public DAgent()
    {
    	
    }

    public DAgent(DCampusWorld state) throws MPIException
    {
        
        double x = state.random.nextDouble() * state.WIDTH;
        double y = state.random.nextDouble() * state.HEIGHT;
        
        position = new DoublePoint(x, y);
        
        content = new int[state.content.length];
        System.arraycopy(state.content, 0, content, 0, state.content.length);
        
        
    }

    public void step(SimState state)
    {
        DCampusWorld campState = (DCampusWorld) state;
        
        assert content.length == campState.content.length;
        for(int i = 0;i<content.length;++i)
        {
        	assert content[i] == campState.content[i];
        }
        
        // update position to coordinate in pixels (see pixelwidth method in GeomGridField)
        double x = state.random.nextDouble() * campState.WIDTH;
        double y = state.random.nextDouble() * campState.HEIGHT;
        DoublePoint loc = new DoublePoint(x, y);
        this.position = loc;
        
        try {
            int dst = campState.partition.toPartitionId(new double[] {loc.c[0], loc.c[1]});
            if (dst != campState.partition.getPid()) {
            	// Need to migrate to other partition, 
            	// remove from current partition 
            	campState.communicator.removeObject(this);
            	campState.queue.migrate(this, dst, loc);           
            } else {        
            	// Set to new location in current partition
            	campState.communicator.setLocation(this, loc);
            	campState.schedule.scheduleOnce(this, 1);
            }
        } catch (Exception e) {
            e.printStackTrace(System.out);
            System.exit(-1);
        }
    }
}

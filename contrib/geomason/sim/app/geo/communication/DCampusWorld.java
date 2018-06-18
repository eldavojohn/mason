/* 
 * Copyright 2011 by Mark Coletti, Keith Sullivan, Sean Luke, and
 * George Mason University Mason University Licensed under the Academic
 * Free License version 3.0
 *
 * See the file "LICENSE" for more information
 *
 * $Id: CampusWorld.java 848 2013-01-08 22:56:43Z mcoletti $
*/
package sim.app.geo.communication;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.planargraph.Node;

import mpi.MPIException;

import java.net.URL;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.text.html.parser.ContentModel;

import sim.engine.Schedule;
import sim.engine.SimState;
import sim.engine.Steppable;
import sim.field.DNonUniformPartition;
import sim.field.DObjectMigratorNonUniform;
import sim.field.continuous.NContinuous2D;
import sim.field.geo.GeomNContinuous2D;
import sim.field.geo.GeomVectorField;
import sim.field.storage.ContStorage;
import sim.field.storage.TestObj;
import sim.io.geo.ShapeFileExporter;
import sim.io.geo.ShapeFileImporter;
import sim.util.Bag;
import sim.util.IntHyperRect;
import sim.util.Timing;
import sim.util.geo.GeomPlanarGraph;
import sim.util.geo.MasonGeometry;

/**
 * This simple example shows how to setup GeomVectorFields and run agents around
 * the fields. The simulation has multiple agents following the walkways at
 * George Mason University. GIS information about the walkways, buildings, and
 * roads provides the environment for the agents. During the simulation, the
 * agents wander randomly on the walkways.
 */
public class DCampusWorld extends SimState
{
	private static final long serialVersionUID = 1L;

	public static final int WIDTH = 300;
	public static final int HEIGHT = 300;

	/** How many agents in the simulation */
	public int numAgents = 9000;

	

	int[] discretizations;
	public NContinuous2D<DAgent> communicator;
	DNonUniformPartition partition;
	public IntHyperRect myPart;
	public DObjectMigratorNonUniform queue;

	int[] content;

	public DCampusWorld(long seed)
	{
		super(seed);

		try
		{
			// Each agent move independently, seems neighborhood is not really
			// playing any role here
			int[] aoi = new int[] { 10, 10 };
			int[] size = new int[] { (int) WIDTH, (int) HEIGHT };
			discretizations = new int[] { 7, 7 };
			partition = DNonUniformPartition.getPartitionScheme(size, true);
			partition.initUniformly(null);
			partition.commit();
			communicator = new NContinuous2D<DAgent>(partition, aoi, discretizations);
			queue = new DObjectMigratorNonUniform(partition);
			myPart = partition.getPartition();
			int length = 0;
			content = new int[length];
			for(int i = 0;i<length;++i)
			{
				content[i] = i;
			}
			System.out.println("content length is " + content.length);
	
		} catch (Exception ex)
		{
			Logger.getLogger(DCampusWorld.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public int getNumAgents()
	{
		return numAgents;
	}

	public void setNumAgents(int n)
	{
		if (n > 0)
			numAgents = n;
	}

	@Override
	public void finish()
	{
		super.finish();

		// Save the agents layer, which has no corresponding originating
		// shape file.
	
	}

	public void start()
	{
		super.start();
		try
		{
			// add all agents into agents field in processor 0
			ContStorage<DAgent> packgeField = new ContStorage<DAgent>(partition.getField(), discretizations);
			if (partition.getPid() == 0)
			{
				for (int i = 0; i < numAgents; ++i)
				{
					DAgent agent = new DAgent(this);
					// Do not add to agents field, we will add that later
					// after distribution
					packgeField.setLocation(agent, agent.position);
				}
			}
			// After distribute is called, communicator will have agents
			communicator.distribute(0, packgeField);

			// Then each processor access these agents, put them in 
			// agents field and schedule them
			Set<DAgent> receivedAgents = ((ContStorage)communicator.getStorage()).m.keySet();
			for (DAgent agent : receivedAgents)
			{
				schedule.scheduleOnce(agent);
			}
		} catch (MPIException e)
		{
			e.printStackTrace();
		}

		schedule.scheduleRepeating(Schedule.EPOCH, 2, new Synchronizer(), 1);

	}

//	void addAgents() throws MPIException
//	{
//		for (int i = 0; i < numAgents / partition.np; i++)
//		{
//			DAgent a = new DAgent(this);
//			communicator.setLocation(a, a.position);
//			schedule.scheduleOnce(a);
//		}
//	}
//
//	@Override
//	public void start()
//	{
//		super.start();
//		agents.clear(); // clear any existing agents from previous runs
//		try
//		{
//			addAgents();
//		} catch (MPIException e)
//		{
//			e.printStackTrace();
//		}
//		agents.setMBR(buildings.getMBR());
//		schedule.scheduleRepeating(Schedule.EPOCH, 2, new Synchronizer(), 1);
//
//		// Ensure that the spatial index is made aware of the new agent
//		// positions. Scheduled to guaranteed to run after all agents moved.
//		schedule.scheduleRepeating(agents.scheduleSpatialIndexUpdater(), Integer.MAX_VALUE, 1.0);
//	}

	

	public static void main(String[] args) throws MPIException
	{
		Timing.setWindow(20);
		doLoopMPI(DCampusWorld.class, args);
		System.exit(0);
	}

	private class Synchronizer implements Steppable
	{
		private static final long serialVersionUID = 1;

		public void step(SimState state)
		{
			DCampusWorld world = (DCampusWorld) state;
			Timing.stop(Timing.LB_RUNTIME);
			// Timing.start(Timing.MPI_SYNC_OVERHEAD);
			try
			{
				world.communicator.sync();
				world.queue.sync();
				// String s = String.format("PID %d Steps %d Number of Agents
				// %d\n", partition.pid, schedule.getSteps(), flockers.size() -
				// flockers.ghosts.size());
				// System.out.print(s);
			} catch (Exception e)
			{
				e.printStackTrace();
				System.exit(-1);
			}
			// TODO
			// Need to put the agent in communicator (halo region) into
			// agents field, but so far we do not have code to easily access
			// all the agents in that region, and since this does not effect
			// the logic of simulation so we do this in future

			// Retrieve the migrated agents from queue and schedule them
			for (Object obj : world.queue)
			{
				DAgent agent = (DAgent) obj;
				world.communicator.setLocation(agent, agent.position);
				schedule.scheduleOnce(agent, 1);
			}
			// Clear the queue
			world.queue.clear();
			// Timing.stop(Timing.MPI_SYNC_OVERHEAD);
		}
	}
}

/* 
 * Copyright 2011 by Mark Coletti, Keith Sullivan, Sean Luke, and
 * George Mason University Mason University Licensed under the Academic
 * Free License version 3.0
 *
 * See the file "LICENSE" for more information
 *
 * $Id: CampusWorld.java 848 2013-01-08 22:56:43Z mcoletti $
*/
package sim.app.geo.dcampusworld;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.planargraph.Node;

import mpi.MPI;
import mpi.MPIException;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import sim.engine.Schedule;
import sim.engine.SimState;
import sim.engine.Steppable;
import sim.field.DUniformPartition;
import sim.field.continuous.Continuous2D;
import sim.field.continuous.DContinuous2D;
import sim.field.geo.GeomVectorField;
import sim.io.geo.ShapeFileExporter;
import sim.io.geo.ShapeFileImporter;
import sim.util.Bag;
import sim.util.geo.GeomPlanarGraph;
import sim.util.geo.MasonGeometry;

/** 
 * This simple example shows how to setup GeomVectorFields and run agents around the fields.  The simulation has 
 * multiple agents following the walkways at George Mason University.  GIS information about the walkways, buildings, 
 * and roads provides the environment for the agents.  During the simulation, the agents wander randomly on the walkways.
 */
public class DCampusWorld extends SimState
{
    private static final long serialVersionUID = -4554882816749973618L;

    public static final int WIDTH = 300; 
    public static final int HEIGHT = 300; 
    
    /** How many agents in the simulation */ 
	public int numAgents = 1000;

    /** Fields to hold the associated GIS information */ 
    public GeomVectorField walkways = new GeomVectorField(WIDTH, HEIGHT);
    public GeomVectorField roads = new GeomVectorField(WIDTH, HEIGHT);
    public GeomVectorField buildings = new GeomVectorField(WIDTH,HEIGHT);

    // where all the agents live
    public GeomVectorField agents = new GeomVectorField(WIDTH, HEIGHT);

    public DContinuous2D communicator;
    public DUniformPartition partition;

    // Stores the walkway network connections.  We represent the walkways as a PlanarGraph, which allows 
    // easy selection of new waypoints for the agents.  
    public GeomPlanarGraph network = new GeomPlanarGraph();
    public GeomVectorField junctions = new GeomVectorField(WIDTH, HEIGHT); // nodes for intersections



    public DCampusWorld(long seed)
    {
        super(seed);

        try
        {
            System.out.println("reading buildings layer");

            // this Bag lets us only display certain fields in the Inspector, the non-masked fields
            // are not associated with the object at all
            Bag masked = new Bag();
            masked.add("NAME");
            masked.add("FLOORS");
            masked.add("ADDR_NUM");

//                System.out.println(System.getProperty("user.dir"));

            // read in the buildings GIS file

            URL bldgGeometry = DCampusWorld.class.getResource("data/bldg.shp");
            ShapeFileImporter.read(bldgGeometry, buildings, masked);

            // We want to save the MBR so that we can ensure that all GeomFields
            // cover identical area.
            Envelope MBR = buildings.getMBR();

            System.out.println("reading roads layer");

            URL roadGeometry = DCampusWorld.class.getResource("data/roads.shp");
            ShapeFileImporter.read(roadGeometry, roads);

            MBR.expandToInclude(roads.getMBR());

            System.out.println("reading walkways layer");

            URL walkWayGeometry = DCampusWorld.class.getResource("data/walk_ways.shp");
            ShapeFileImporter.read(walkWayGeometry, walkways);

            MBR.expandToInclude(walkways.getMBR());

            System.out.println("Done reading data");

            // Now synchronize the MBR for all GeomFields to ensure they cover the same area
            buildings.setMBR(MBR);
            roads.setMBR(MBR);
            walkways.setMBR(MBR);

            network.createFromGeomField(walkways);

            addIntersectionNodes(network.nodeIterator(), junctions);

        } catch (FileNotFoundException ex)
        {
            Logger.getLogger(DCampusWorld.class.getName()).log(Level.SEVERE, null, ex);
        }

    }


    public int getNumAgents() { return numAgents; } 
    public void setNumAgents(int n) { if (n > 0) numAgents = n; } 

    /**
     * Add agents to the simulation and to the agent GeomVectorField. Note that
     * each agent does not have any attributes.
     */
    void addAgents()
    {
        for (int i = 0; i < numAgents; i++)
        {
            DAgent a = new DAgent(this);
            agents.addGeometry(a.getGeometry());
            communicator.setObjectLocation(a, a.position);
        }
    }

    @Override
    public void finish()
    {
        super.finish();

        // Save the agents layer, which has no corresponding originating
        // shape file.
        ShapeFileExporter.write("agents", agents);
    }

    
    @Override
    public void start()
    {
        super.start();
        
        partition = new DUniformPartition(new int[] {(int)WIDTH, (int)HEIGHT});
        communicator = new DContinuous2D(10 / 1.5, WIDTH, HEIGHT, 10, partition, this.schedule);

        schedule.scheduleRepeating(Schedule.EPOCH, 0, new Synchronizer(), 1);

        agents.clear(); // clear any existing agents from previous runs
        addAgents();
        agents.setMBR(buildings.getMBR());

        // Ensure that the spatial index is made aware of the new agent
        // positions.  Scheduled to guaranteed to run after all agents moved.
        schedule.scheduleRepeating( agents.scheduleSpatialIndexUpdater(), Integer.MAX_VALUE, 1.0);
    }



    /** adds nodes corresponding to road intersections to GeomVectorField
     *
     * @param nodeIterator Points to first node
     * @param intersections GeomVectorField containing intersection geometry
     *
     * Nodes will belong to a planar graph populated from LineString network.
     */
    private void addIntersectionNodes(Iterator nodeIterator, GeomVectorField intersections)
    {
        GeometryFactory fact = new GeometryFactory();
        Coordinate coord = null;
        Point point = null;
        int counter = 0;

        while (nodeIterator.hasNext())
            {
                Node node = (Node) nodeIterator.next();
                coord = node.getCoordinate();
                point = fact.createPoint(coord);

                junctions.addGeometry(new MasonGeometry(point));
                counter++;
            }
    }

    public static void main(String[] args) throws MPIException
    {
    	MPI.Init(args);
        doLoop(DCampusWorld.class, args);
        MPI.Finalize();
        System.exit(0);
    }
    
    private class Synchronizer implements Steppable {
        private static final long serialVersionUID = 1;

        public void step(SimState state) {
            try {
                communicator.sync();
//                String s = String.format("PID %d Steps %d Number of Agents %d\n", partition.pid, schedule.getSteps(), flockers.size() - flockers.ghosts.size());
//                System.out.print(s);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}

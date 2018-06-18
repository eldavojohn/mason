package sim.field.geo;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import sim.field.continuous.DContinuous2D;
import sim.field.geo.GeomField;
import sim.field.grid.Grid2D;
import sim.util.Double2D;

public class GeomDContinuous2D
{
	/** The minimum bounding rectangle (MBR) of all the stored geometries. */
    public Envelope MBR;
    
    /** Holds the origin for drawing; used to handle zooming and scrolling */
    public double drawX, drawY;
    
    public DContinuous2D field = null;
    
    /** width of grid point in projection coordinate system
    *
    * @see GeomGridField.setGrid()
    * @see GeomGridField.setMBR()
    */
   private double pixelWidth = 0.0;

   /** height of grid point in projection coordinate system
    *
    * @see GeomGridField.setGrid()
    * @see GeomGridField.setMBR()
    */
   private double pixelHeight = 0.0;

    public GeomDContinuous2D()
    {
        MBR = new Envelope();
        drawX = drawY = 0;
    }
    
    public GeomDContinuous2D(DContinuous2D continuous2d)
    {
    	this();
        setField(continuous2d);
    }
    
	/** The field dimensions
    *
    * Used for computing scale.
    *
    */
   
   public double getFieldWidth() { 
	   return field.getWidth();
   } 
   
   public double getFieldHeight() {
	   return field.getHeight();
   }

    

    /** delete contents */
    public void clear()
    {
        MBR = new Envelope();
        drawX = drawY = 0;
        field = null;
    }

    /** Returns the width of the MBR. */
    public double getWidth()
    {
        return MBR.getWidth();
    }

    /** Returns the height of the MBR. */
    public double getHeight()
    {
        return MBR.getHeight();
    }

    /** Returns the minimum bounding rectangle (MBR) */
    public final Envelope getMBR()
    {
        return MBR;
    }

    
    
    /** Height of pixels in units of the underlying coordinate reference system */
    public double getPixelHeight()
    {
        return pixelHeight;
    }

    /** Set heigh of pixels in units of the underlying coordinate reference system */
    public void setPixelHeight(double pixelHeight)
    {
        this.pixelHeight = pixelHeight;
    }

    /** Width of pixels in units of the underlying coordinate reference system */
    public double getPixelWidth()
    {
        return pixelWidth;
    }

    /** Set pixel width in units of underlying coordinate reference system */
    public void setPixelWidth(double pixelWidth)
    {
        this.pixelWidth = pixelWidth;
    }
   

    public final DContinuous2D getField()
    {
        return field;
    }

    
    public final void setField(DContinuous2D newField)
    {
        field = newField;

        setPixelWidth(field.getWidth());
        setPixelHeight(field.getHeight());
    }
    
    
    /** Set the MBR */
    public void setMBR(Envelope MBR)
    {
    	this.MBR = MBR;

        // update pixelWidth and pixelHeight iff grid is set
        if (field != null)
        {
            setPixelWidth(MBR.getWidth() / getFieldWidth());
            setPixelHeight(MBR.getHeight() / getFieldHeight());
        }
    }
    
    /**
     * @param p point
     * @return x grid coordinate for cell 'p' is in
     */
    public double toXCoord(final Point p)
    {
        return (p.getX() - getMBR().getMinX()) / getPixelWidth();
    }

    /**
     *
     * @param x Coordinate in base projection
     * @return x grid coordinate for cell 'x'
     */
    public double toXCoord(final double x)
    {
        return (x - getMBR().getMinX()) / getPixelWidth();
    }


    /**
     * @param p point
     * @return y grid coordinate for cell 'p' is in
     */
    public double toYCoord(final Point p)
    {
        // Note that we have to flip the y coordinate because the origin in
        // MASON is in the upper left corner.
        return (getMBR().getMaxY() - p.getY()) / getPixelHeight();
    }

    /**
     *
     * @param y coordinate in base projection
     * @return y grid coordinate for cell 'y' is in
     */
    public double toYCoord(final double y)
    {
        // Note that we have to flip the y coordinate because the origin in
        // MASON is in the upper left corner.
        return (getMBR().getMaxY() - y) / getPixelHeight();
    }

    
    public boolean setObjectLocation(Object obj, final Double2D loc)
    {
    	return field.setObjectLocation(obj, loc);
    }
    
    public void sync() 
    {
    	field.sync();
    }
}

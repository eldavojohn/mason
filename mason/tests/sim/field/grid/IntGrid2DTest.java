package tests.sim.field.grid;
import java.util.Collections;

import org.junit.Test;
import static org.junit.Assert.*;

import java.awt.print.Printable;
import java.util.ArrayList;

import sim.field.grid.IntGrid2D;

public class IntGrid2DTest
{
	@Test
	public void testSetAndGet()
	{
		IntGrid2D grid2d = new IntGrid2D(3, 3, 0);
		assertEquals(0, grid2d.get(1, 1));
		
		grid2d.set(1, 1, 10);
		assertEquals(10, grid2d.get(1, 1));
	}
	
	@Test
	public void testSetTo()
	{
		// TODO Auto-generated method stub

	}
	
	@Test
	public void testMean() {
		IntGrid2D grid2d = new IntGrid2D(3, 3, 0);
		assertEquals(0.0, grid2d.mean(), 1e-5);

		int value = 1;
		for(int i = 0;i<grid2d.getWidth();++i)
		{
			for(int j = 0;j<grid2d.getHeight();++j)
			{
				grid2d.field[i][j] = value++;
			}
		}
		assertEquals(5.0, grid2d.mean(), 1e-5);

		grid2d = new IntGrid2D(0, 0, 0);
		assertEquals(0.0, grid2d.mean(), 1e-5);
	}

	@Test
	public void testMax() {
		IntGrid2D grid2d = new IntGrid2D(3, 3, 0);
		ArrayList<Integer> integerList = new ArrayList<Integer>();
		for (int i = 0;i < 9;++i)
			integerList.add(i);
		Collections.shuffle(integerList);
		int index = 0;
		for(int i = 0;i < grid2d.getWidth();++i)
		{
			for(int j = 0;j<grid2d.getHeight();++j)
			{
				grid2d.field[i][j] = integerList.get(index++);
			}
		}
		assertEquals(8, grid2d.max());
	}
	
	@Test
	public void testMin() {
		IntGrid2D grid2d = new IntGrid2D(3, 3, 0);
		ArrayList<Integer> integerList = new ArrayList<Integer>();
		for (int i = 0;i < 9;++i)
			integerList.add(i);
		Collections.shuffle(integerList);
		int index = 0;
		for(int i = 0;i < grid2d.getWidth();++i)
		{
			for(int j = 0;j<grid2d.getHeight();++j)
			{
				grid2d.field[i][j] = integerList.get(index++);
			}
		}
		assertEquals(0, grid2d.min());
	}


}

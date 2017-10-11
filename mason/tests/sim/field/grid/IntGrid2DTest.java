package tests.sim.field.grid;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.Test;

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
	
	@Rule public ExpectedException thrown = ExpectedException.none();
	
	@Test
	public void testSetTo()
	{
		IntGrid2D grid2d = new IntGrid2D(3, 3, 0);
		grid2d.setTo(10);
		for(int i = 0;i<grid2d.getWidth();++i)
		{
			for(int j = 0;j<grid2d.getHeight();++j)
				assertEquals(10, grid2d.field[i][j]);
		}
		
		// Test with null array
		int[][] nullArray = null;
		thrown.expect(RuntimeException.class );
	    thrown.expectMessage("IntGrid2D set to null field.");
	    
	    grid2d.setTo(nullArray);
		
	    // Test with non-rectangular array
	    int[][] nonRectangularArray = new int[2][];
	    nonRectangularArray[0] = new int[]{0, 1, 2};
	    nonRectangularArray[1] = new int[]{4, 5};
	    thrown.expect(RuntimeException.class);
	    thrown.expectMessage("IntGrid2D initialized with a non-rectangular field.");
	    grid2d.setTo(nonRectangularArray);
	    
	    // Test with rectangular array
	    int[][] rectangularArray = new int[][]{{0, 1, 2}, {4, 5, 6}};
	    grid2d.setTo(rectangularArray);
	    for(int i = 0;i<grid2d.getWidth();++i)
	    {
	    	for(int j = 0;j<grid2d.getHeight();++j)
	    	{
	    		assertEquals(rectangularArray[i][j], grid2d.field[i][j]);
	    	}
	    }
	    
	    // Test with IntGrid2D
	    IntGrid2D secondGrid2d = new IntGrid2D(3, 5, 10);
	    secondGrid2d.setTo(grid2d);
	    for(int i = 0;i<grid2d.getWidth();++i)
	    {
	    	for(int j = 0;j<grid2d.getHeight();++j)
	    	{
	    		assertEquals(rectangularArray[i][j], secondGrid2d.field[i][j]);
	    	}
	    }
	}
	
	@Test
	public void testToArray() {
		int[][] rectangularArray = new int[][]{{0, 1, 2}, {3, 4, 5}};
		int[] flatArray = new int[]{0, 1, 2, 3, 4, 5};
		IntGrid2D grid2d = new IntGrid2D(rectangularArray);
		assertArrayEquals(grid2d.toArray(), flatArray);
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
	public void testUpperBoundAndLowerBound() {
		int[][] array = new int[][]{{0, 1, 2}, {4, 5, 6}};
		IntGrid2D grid2d = new IntGrid2D(array);
		grid2d.upperBound(4);
		for(int i = 0;i < grid2d.getHeight();++i)
		{
			assertEquals(4, grid2d.field[1][i]);
		}
		grid2d.lowerBound(4);
		for(int i = 0;i < grid2d.getHeight();++i)
		{
			assertEquals(4, grid2d.field[0][i]);
		}
	}
	
	@Test
	public void testAdd() {
		int[][] array = new int[][]{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}};
		IntGrid2D grid2d = new IntGrid2D(array);
		grid2d.add(1);
		int[] results = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
		assertArrayEquals(grid2d.toArray(), results);
		
		results = new int[]{-1, 0, 1, 2, 3, 4, 5, 6, 7};
		grid2d.add(new IntGrid2D(3, 3, -2));
		assertArrayEquals(grid2d.toArray(), results);
	}
	
	@Test
	public void testMultiply() {
		int[][] array = new int[][]{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}};
		IntGrid2D grid2d = new IntGrid2D(array);
		grid2d.multiply(2);
		int[] results = new int[]{0, 2, 4, 6, 8, 10, 12, 14, 16};
		assertArrayEquals(grid2d.toArray(), results);
		
		results = new int[]{0, -2, -4, -6, -8, -10, -12, -14, -16};
		grid2d.multiply(new IntGrid2D(3, 3, -1));
		assertArrayEquals(grid2d.toArray(), results);
	}
	
	@Test
	public void replaceAll() {
		int[][] array = new int[][]{{0, 1, 0}, {1, 0, 1}, {0, 1, 0}};
		IntGrid2D grid2d = new IntGrid2D(array);
		
		grid2d.replaceAll(1, 2);
		int results[] = new int[]{0, 2, 0, 2, 0, 2, 0, 2, 0};
		
		assertArrayEquals(results, grid2d.toArray());
	}
	


}

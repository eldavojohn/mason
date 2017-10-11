package tests.sim.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;

import javax.management.BadBinaryOpValueExpException;

import static org.junit.Assert.*;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.Test;

import sim.util.Bag;

public class BagTest
{
	@Test
	public void testConstructor() {
		Bag bag = new Bag();
		assertEquals(1, bag.objs.length);
		assertEquals(0, bag.numObjs);
		
		bag = new Bag(10);
		assertEquals(10, bag.objs.length);
		assertEquals(0, bag.numObjs);
		
		Bag nullBag = null;
		bag = new Bag(nullBag);
		assertEquals(1, bag.objs.length);
		assertEquals(0, bag.numObjs);
		
		Bag otherBag = new Bag(5);
		otherBag.objs = new Object[]{"one", "two", "three", "four", "five"};
		otherBag.numObjs = 5;
		bag = new Bag(otherBag);
		assertArrayEquals(otherBag.objs, bag.objs);
		assertEquals(5, bag.numObjs);
	}
	
	@Test
	public void testAddAll() {
		String[] strArray = new String[]{"one", "two", "three", "four", "five"};
		Bag bag = new Bag();
		bag.addAll(strArray);
		assertEquals(5, bag.numObjs);
		assertArrayEquals(strArray, bag.objs);
		
		ArrayList<String> strList = new ArrayList<String>(Arrays.asList(strArray));
		bag = new Bag();
		bag.addAll(strArray);
		assertEquals(5, bag.numObjs);
		assertArrayEquals(strArray, bag.objs);
		
		
	}
	
	@Test
	public void testAdd() {
		Bag bag = new Bag();
		String[] strArray = new String[]{"one", "two", "three"}; 
		for(int i = 0;i<strArray.length;++i)
			bag.add(strArray[i]);
		
		for(int i = 0;i<bag.objs.length;++i)
			assertEquals(strArray[i], bag.objs[i]);
	}
	
}

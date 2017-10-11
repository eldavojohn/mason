package tests.sim;

import org.junit.runners.Suite;
import org.junit.runner.RunWith;
import tests.sim.field.grid.*;
import tests.sim.util.*;


@RunWith(Suite.class)
@Suite.SuiteClasses({
    IntGrid2DTest.class,
    BagTest.class
})

public class AllTests
{

}

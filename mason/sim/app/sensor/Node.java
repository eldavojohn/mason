package sim.app.sensor;

import sim.engine.*;
import sim.field.continuous.*;
import sim.util.*;

public class Node implements Steppable {

	public Double2D loc;
	public int id;
	public double val;

	double stdev = 0, avg = 0, pwrSumAvg = 0;
	int n = 0;

	Sensor sim;
	double mixing;
	double threshold;
	double lrange;
	double srange;

	boolean useLRRadio = false;

	public Node(int id, double val, Double2D loc, SimState state) {
		sim = (Sensor)state;
		this.id = id;
		this.val = val;
		this.loc = loc;
		mixing = sim.mixing;
		threshold = sim.threshold;
		lrange = sim.lrange;
		srange = sim.srange;
	}

	public void step(SimState state) {
		//Sensor sim = (Sensor)state;

		if (useLRRadio) {
			setLRRadio(false);
			Node dst = select(sim.LRField, lrange);
			if (dst != null)
				send(dst);
		}

		Node dst = select(sim.field, srange);
		if (dst == null) {
			System.out.printf("Node %d has no neighbor\n", id);
			System.exit(-1);
		}
		send(dst);
	}

	public void send(Node dst) {
		val = mixing * val + (1 - mixing) * dst.val;
		update();
		if (stdev <= threshold)
			setLRRadio(true);
	}

	public Node select(Continuous2D f, double r) {
		Bag b = f.getNeighborsExactlyWithinDistance(loc, r, false);
		b.shuffle(sim.random);
		return (Node)b.pop();
	}

	public void update() {
		n++;
		avg += (val - avg) / n;
		pwrSumAvg += (val * val - pwrSumAvg) / n;
		stdev = Math.sqrt((pwrSumAvg * n - n * avg * avg) / (n - 1));
	}

	public void setLRRadio(boolean use) {
		useLRRadio = use;
		if (use) 
			sim.LRField.remove(this);
		else
			sim.LRField.setObjectLocation(this, loc);
	}
}
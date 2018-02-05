package sim.app.sensor;

import sim.engine.*;
import sim.field.continuous.*;
import sim.util.*;

public class Sensor extends SimState {

	double mixing;
	double lrange;
	double srange;
	double threshold;

	int num_nodes;
	double width, height;

	double maxval = 100;

	Continuous2D field, LRField;

	Node[] nodes;

	public Sensor(long seed) {
		this(seed, 200, 200, 100, 10, 100, 0.5, 0.1);
	}

	public Sensor(long seed, double width, double height, double lrange, double srange, int num_nodes, double mixing, double thratio) {
		super(seed);
		this.width = width;
		this.height = height;
		this.lrange = lrange;
		this.srange = srange;
		this.num_nodes = num_nodes;
		this.mixing = mixing;
		this.threshold = maxval * thratio;

		field = new Continuous2D(srange/1.5, width, height);
		LRField = new Continuous2D(lrange/1.5, width, height);
	}

	private Double2D randLoc() {
		double x = random.nextDouble() * width;
		double y = random.nextDouble() * height;
		return new Double2D(x, y);
	}

	private void createNodes(int n) {
		nodes = new Node[100];
		for (int i = 0; i < n; i++) {
			nodes[i] = new Node(i, random.nextDouble() * maxval, randLoc(), this);
			field.setObjectLocation(nodes[i], nodes[i].loc);
		}

		for (int i = 0; i < n; i++) {
			while (field.getNeighborsExactlyWithinDistance(nodes[i].loc, srange, false).size() == 0) {
				nodes[i].loc = randLoc();
				field.setObjectLocation(nodes[i], nodes[i].loc);
			}
		}
	}

	public void start() {
		createNodes(num_nodes);
		for (int i = 0; i < num_nodes; i++)
			schedule.scheduleRepeating(nodes[i]);
	}

	public static void main(String[] args) {
		doLoop(Sensor.class, args);
		System.exit(0);
	}
}
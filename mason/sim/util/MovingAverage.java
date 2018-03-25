package sim.util;

public class MovingAverage {
	double average;
	double[] queue;
	int capacity, count, st;

	public MovingAverage(int capacity) {
		this.queue = new double[capacity];
		this.capacity = capacity;
	}

	public double next(double val) {
		if (count < capacity) {
			queue[st + count] = val;
			average = average * ((double)count / (count + 1)) + val / (count + 1);
			count++;
		} else {
			average += (val - queue[st]) / capacity;
			queue[st] = val;
			st = (st + 1) % capacity;
		}

		return average;
	}

	public double average() {
		return average;
	}

	public static void main(String args[]) {
		MovingAverage a = new MovingAverage(4);
		System.out.printf("1 \t Want %g \t Got %g \n", 10.0, a.next(10));
		System.out.printf("2 \t Want %g \t Got %g \n", 15.0, a.next(20));
		System.out.printf("3 \t Want %g \t Got %g \n", 15.0, a.next(15));
		System.out.printf("4 \t Want %g \t Got %g \n", 12.5, a.next(5));
		System.out.printf("5 \t Want %g \t Got %g \n", 10.0, a.next(0));
		System.out.printf("6 \t Want %g \t Got %g \n", 12.5, a.next(30));
		System.out.printf("7 \t Want %g \t Got %g \n", 11.25, a.next(10));
		System.out.printf("8 \t Want %g \t Got %g \n", 15.0, a.next(20));
		System.out.printf("9 \t Want %g \t Got %g \n", 27.5, a.next(50));
		System.out.printf("10 \t Want %g \t Got %g \n", 22.5, a.next(10));
		System.out.printf("11 \t Want %g \t Got %g \n", 25.0, a.next(20));
		System.out.printf("12 \t Want %g \t Got %g \n", 23.75, a.next(15));
		System.out.printf("13 \t Want %g \t Got %g \n", 12.5, a.next(5));
	}
}
package sim.util;

import java.util.concurrent.TimeUnit;

public class TimingStat {

	long cnt;
	double avg, min, max, var;
	MovingAverage mav;
	int cap;

	final long conv;

	public TimingStat(int cap) {
		this(cap, TimeUnit.MILLISECONDS);
	}

	public TimingStat(int cap, TimeUnit u) {
		this.cap = cap;
		this.conv = TimeUnit.NANOSECONDS.convert(1L, u);
		reset();
	}

	public void add(double val) {
		min = Math.min(min, val);
		max = Math.max(max, val);

		double avg_old = avg;
		avg += (val - avg) / ++cnt;
		var += (val - avg_old) * (val - avg);

		mav.next(val);
	}

	public void reset() {
		mav = new MovingAverage(cap);
		cnt = 0;
		min = Double.MAX_VALUE;
		max = 0;
		avg = 0;
		var = 0;
	}

	public long getCount() {
		return cnt;
	}

	public double getMean() {
		return avg / conv;
	}

	public double getMin() {
		return min / conv;
	}

	public double getMax() {
		return max / conv;
	}

	public double getStdev() {
		if (cnt > 1)
			return Math.sqrt(var / (cnt - 1)) / conv;
		return 0;
	}

	public double getMovingAverage() {
		return mav.average() / conv;
	}

	public double getMovingStdev() {
		return mav.stdev() / conv;
	}

	public String toString() {
		return String.format("%d\t%f\t%f\t%f\t%f\t%f\t%f",
		                     getCount(),
		                     getMin(),
		                     getMax(),
		                     getMean(),
		                     getStdev(),
		                     getMovingAverage(),
		                     getMovingStdev()
		                    );
	}
}
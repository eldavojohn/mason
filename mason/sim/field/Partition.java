package sim.field;

public class Partition {
	public double[] ul;
	public double[] br;
	public int pid;

	public Partition(final double[] ul, final double[] br, final int pid) {
		this.ul = ul;
		this.br = br;
		this.pid = pid;
	}
}
package sim.util;

import mpi.*;

import java.util.*;
import java.io.*;
import java.net.InetAddress;

public class Launcher {

	List<String> mpiDefaultArgs = Arrays.asList("-Djava.library.path=/usr/local/lib", "-cp", ".:/usr/local/lib/mpi.jar");

	int np;
	String hostfile, jobArgs[], jobClass, jobCommand = "java";
	Info info;
	Intercomm jobComm;

	LogServer ls = null;
	int logServPort = -1;
	String logServAddr;

	public Launcher(int np, String hostfile, String jobClass, String[] jobArgs) {
		this.np = np;
		this.hostfile = hostfile;
		this.jobClass = jobClass;
		this.jobArgs = jobArgs;
	}

	public Launcher(String args[]) {
		if (args.length < 3)
			throw new IllegalArgumentException("Not enough arguments");

		this.np = Integer.parseInt(args[0]);
		this.hostfile = args[1];
		this.jobClass = args[2];
		this.jobArgs = Arrays.copyOfRange(args, 3, args.length);
	}

	public void startLogServer() throws IOException {
		if (logServPort < 0) {
			ls = new LogServer();
			logServPort = ls.port;
		} else
			ls = new LogServer(logServPort);

		logServAddr = InetAddress.getLocalHost().getHostAddress();

		ls.start();
	}

	public void startMPIJobs() throws MPIException {
		if (MPI.isInitialized())
			throw new RuntimeException("TODO: MPI can only be initialized once");

		MPI.Init(new String[0]);

		info = new Info();
		info.set("hostfile", hostfile);

		jobComm = MPI.COMM_WORLD.spawn(jobCommand, getMPIJobArgs(), np, info, 0, null);

		MPI.Finalize();
	}

	public void tearddown() {
		// Close the server socket to bring down the LogServer
		ls.closeSock();

		// TODO other clean up
	}

	// Concat the following pieces to construct the command arguments
	private String[] getMPIJobArgs() {
		// Default MPI args
		List<String> allArgs = new ArrayList<String>(mpiDefaultArgs);

		// Jobclass
		allArgs.add(jobClass);

		// logserver address/port
		allArgs.add("-logserver");
		allArgs.add(logServAddr);
		allArgs.add("-logport");
		allArgs.add(Integer.toString(logServPort));

		// jobArgs
		allArgs.addAll(Arrays.asList(jobArgs));

		return allArgs.toArray(new String[0]);
	}

	public static void main(String args[]) {

		Launcher l = new Launcher(args);

		// Create log server
		try {
			l.startLogServer();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		// TODO: Create other utils

		// start MPI jobs
		try {
			l.startMPIJobs();
		} catch (MPIException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		// TODO: Create visualization & control
	}
}
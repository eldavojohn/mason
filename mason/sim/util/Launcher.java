package sim.util;

import mpi.*;

import java.util.Properties;
import java.util.Enumeration;
import java.util.Arrays;
import java.io.*;
import java.net.InetAddress;

public class Launcher {

	String[] mpiDefaultArgs = {"-np", "1"};

	int np;
	String hostfile, jobCommand, jobArgs[];
	Info info;
	Intercomm jobComm;

	LogServer ls = null;
	int logServPort = -1;
	String logServAddr;

	public Launcher(int np, String hostfile, String jobCommand, String[] jobArgs) {
		this.np = np;
		this.hostfile = hostfile;
		this.jobCommand = jobCommand;
		this.jobArgs = jobArgs;
	}

	public Launcher(String args[]) {
		if (args.length < 3)
			throw new IllegalArgumentException("Not enough arguments");

		this.np = Integer.parseInt(args[0]);
		this.hostfile = args[1];
		this.jobCommand = args[2];
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

		MPI.Init(mpiDefaultArgs);

		info = new Info();
		info.set("hostfile", hostfile);

		jobComm = MPI.COMM_WORLD.spawn(jobCommand, jobArgs, np, info, 0, null);

		MPI.Finalize();
	}

	public static void main(String args[]) {

		//Launcher l = new Launcher(3, "/home/hwang17/Workspace/mason/mason/hostfile", "uptime", null);
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
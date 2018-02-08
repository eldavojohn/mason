package sim.util;

import java.util.*;
import java.net.*;
import java.io.*;

class LogServer extends Thread {
	private ServerSocket ssock;
	private Socket csock;

	public final int port;
	
	public LogServer(final int port) throws IOException {
		this.port = port;
		this.ssock = new ServerSocket(port);
	}

	public LogServer() throws IOException {
		this.ssock = new ServerSocket(0);
		this.port = ssock.getLocalPort();
	}

	public void run() {
		while (true) {
			try {
				csock = ssock.accept();
				new LogServerHandler(csock).start();
			} catch (IOException e) {
				break;
			}
		}

		try {
			ssock.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		LogServer es = null;
		int port = 5667;
		
		try {
			es = new LogServer(port);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		es.start();
	}
}

class LogServerHandler extends Thread {
	Socket socket;
	BufferedReader br;

	public LogServerHandler(Socket s) throws IOException {
		this.socket = s;
		InputStream in = s.getInputStream();
		InputStreamReader isr = new InputStreamReader(in);
		this.br = new BufferedReader(isr);
	}

	public void run() {
		String line;
		try {
			while ((line = br.readLine()) != null)
				handle(line);
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private synchronized void handle(String line) {
		System.out.println(line);
	}
}
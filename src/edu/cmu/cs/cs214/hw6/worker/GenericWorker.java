package edu.cmu.cs.cs214.hw6.worker;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;

import edu.cmu.cs.cs214.hw6.master.Master;

/**
 * Extensible class that handles much of the shared functionality between
 *  the two worker types. 
 *  
 * @author msebek
 *
 */
public abstract class GenericWorker implements Worker, Runnable {

	private boolean DEBUG_RMI = true;

	protected static final String DEFAULT_MASTER_HOST = "localhost";
	protected static final int DEFAULT_MASTER_PORT = 15214;

	protected Random r = new Random();
	
	protected final String masterHostname;
	protected String name;

	// RMI Stuff
	protected Registry remoteRegistry;
	protected Worker stub;
	protected Master remoteMaster = null;

	protected GenericWorker(String masterHostname) {
		this.masterHostname = masterHostname;
	}
	
	protected void log(String message) {
		System.out.printf("[%s][%s]\n", name, message);
	}

	protected void logErr(String message) {
		System.err.printf("[%s][%s]\n", name, message);
	}

	@Override
	public void run() {
		start();
	}

	/**
	 * Call either registerReduceWorker, or registerMapWorker depending on which
	 * subclass you are. Register with your name.
	 */
	public abstract void registerWorker(Master remoteMaster) throws RemoteException;

	public abstract int getObjectServePort();

	public void start() {
		// Connect to Master.
		int retries = 0;
		int retry_limit = 6;
		while (retries < retry_limit) {
			try {
				remoteRegistry = LocateRegistry.getRegistry(this.masterHostname, DEFAULT_MASTER_PORT);
				remoteMaster = (Master) remoteRegistry.lookup("master");
		
				if (DEBUG_RMI)
					log("Connected to remote RMI.");

				// Register with Map/Reduce worker with master (not registry stuff)
				registerWorker(remoteMaster);
				return;
			} catch (IOException e1) {
				log("Could not connect to " + this.masterHostname + ", port "
						+ DEFAULT_MASTER_PORT + " Retrying...");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					assert(false);
				}
			} catch (NotBoundException e) {
				log("Server not online yet:" + this.masterHostname + ", port "
						+ DEFAULT_MASTER_PORT + " Retrying...");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException ie) {
				} finally {
				}
			}
		}
		// Hard Exit. Nothing to clean up.
		assert(false);
	}

    /**
	 * Unregister with Worker Server using RMI
	 * @param nickname Nick to register with server. 
	 * @throws RemoteException
	 */
	protected void closeOutRMIWorker() {
		// Copied from stack overflow "remotely shut down RMI server"
		try {
			remoteRegistry.unbind(this.name);
			UnicastRemoteObject.unexportObject(this, true);
		} catch (NotBoundException e) {
			logErr("Worker not bound...this should not happen. Close called twice?");
			e.printStackTrace();
			assert(false);
		} catch (RemoteException e) {
			e.printStackTrace();
			assert(false);
		}
		
		if(DEBUG_RMI) log("Shutting down...");
	}
	

	@Override
	public void shutdownWorker() throws RemoteException {
		closeOutRMIWorker();
	}
	
	@Override
	public boolean checkUp() throws RemoteException {
		return true;
	}
}

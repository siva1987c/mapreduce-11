package edu.cmu.cs.cs214.hw6.master;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import edu.cmu.cs.cs214.hw6.MapTask;
import edu.cmu.cs.cs214.hw6.ReduceTask;
import edu.cmu.cs.cs214.hw6.TaskFactory;
import edu.cmu.cs.cs214.hw6.worker.MapWorker;
import edu.cmu.cs.cs214.hw6.worker.ReduceWorker;

/**
 * Abstract Master handles all the communication and tasking grossness. 
 * 
 * @author msebek
 * 
 */
public abstract class RawMaster implements Master {

	private static boolean DEBUG_RMI = true;
	private final Random r = new Random();

	protected MapTask map=null;
	protected ReduceTask red=null;

	protected final HashMap<String, MapWorker> mapWorkers = new HashMap<String, MapWorker>();
	protected final HashMap<String, ReduceWorker> reduceWorkers = new HashMap<String, ReduceWorker>();




	// RMI Shizz
	private Registry registry;
	private Master stub;
	private int port;

	protected String name;
	protected static int MASTER_PORT = 15214;
	protected final String myHostName;

	/**
	 * Start the Master server.
	 * 
	 * @param allTablets
	 *            List of the tablets we're expected to map over.
	 * @param name
	 *            Name of the master (mainly used for display)
	 * @param m
	 *            MapTask to execute.
	 * @param r
	 *            ReduceTask to execute.
	 */
	public RawMaster(int port, String name,
			String myOwnHostName) {
		this.myHostName = myOwnHostName;
		this.port = port;
		this.name = name;
		this.map = null;
		this.red = null;
	}

	/**
	 * Set up Master server to recieve connections from workers.
	 */
	public void start() {
		try {
			System.setProperty("java.rmi.server.hostname", myHostName);
			log("MasterHostName[" + myHostName + "]");
			// Set up registry
			this.registry = LocateRegistry.createRegistry(MASTER_PORT);

			// Bind the server.
			stub = (Master) UnicastRemoteObject.exportObject(this,
					MASTER_PORT + 1);
			// stub = (Master) UnicastRemoteObject.exportObject(;
			registry.bind("master", stub);
			if (DEBUG_RMI)
				log("RMI Server Opened.");
		} catch (RemoteException e) {
			e.printStackTrace();
			assert (false);
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
			assert (false);
		}

		logErr("Master waiting for incoming connections...");
		return;
	}

	/**
	 * Shut down the cluster, so we can exit cleanly.
	 */
	private void closeOutRMIServer() {
		try {
			// Closeout Workers
			for (MapWorker mw : mapWorkers.values()) {
				mw.shutdownWorker();
			}

			for (ReduceWorker rw : reduceWorkers.values()) {
				rw.shutdownWorker();
			}

			registry.unbind("master");
			UnicastRemoteObject.unexportObject(this, true);

			logErr("Shutting down, check output.txt for results.");
		} catch (NotBoundException e) {
			logErr("Server not bound...this should not happen. Close called twice?");
			e.printStackTrace();
			assert (false);
		} catch (AccessException e) {
			e.printStackTrace();
			assert (false);
		} catch (RemoteException e) {
			e.printStackTrace();
			assert (false);
		}
	}

	public synchronized void registerMapWorker(String mapWorkerName,
			MapWorker mapper) {
		mapWorkers.put(mapWorkerName, mapper);
	}

	public synchronized void registerReduceWorker(String reduceWorkerName,
			ReduceWorker redder) {
		reduceWorkers.put(reduceWorkerName, redder);

	}

	protected synchronized void log(String message) {
		System.out.printf("[%s][%s]\n", name, message);
	}

	protected synchronized void logErr(String message) {
		System.err.printf("[%s][%s]\n", name, message);
	}

	public void run() {
		start();
	}

	public List<ReduceWorker> getReduceWorkers() throws RemoteException {
		return new ArrayList<ReduceWorker>(this.reduceWorkers.values());
	}

}
         

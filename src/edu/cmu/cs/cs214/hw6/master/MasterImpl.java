package edu.cmu.cs.cs214.hw6.master;

import java.io.FileWriter;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import edu.cmu.cs.cs214.hw6.MapTask;
import edu.cmu.cs.cs214.hw6.ReduceTask;
import edu.cmu.cs.cs214.hw6.worker.MapWorker;
import edu.cmu.cs.cs214.hw6.worker.ReduceWorker;

/**
 * Master Server of map-reduce framework.
 * 
 * (hosts the RMI server for everyone)
 * 
 * Responsibilities: - Receiving Map task from client - Load the map and reduce
 * tasks as arguments. - Distributing Map task to Slaves - Coordinating Shuffle
 * - Tell which workers should get which pieces. - How to partition reduce?
 * 
 * 
 * 
 * @author msebek
 * 
 */
public class MasterImpl implements Master, Runnable {

	private static boolean DEBUG_RMI = true;
	private final Random r = new Random();

	private MapTask map;
	private ReduceTask red;

	private final HashMap<String, MapWorker> mapWorkers = new HashMap<String, MapWorker>();
	private final HashMap<String, ReduceWorker> reduceWorkers = new HashMap<String, ReduceWorker>();

	// Map(workername -> tabletname)
	private Map<String, List<String>> assignments;
	private Map<String, List<String>> remainingAssignments;

	private boolean running = false;

	// Reduce
	private Set<String> reducesNotDone;

	// private final List<String> allWorkers; // workers who have not connected
	// yet
	private final List<String> allTablets;
	private Set<String> tabletsHere = new HashSet<String>();
	// Results
	private Map<String, String> finalAnswers = new TreeMap<String, String>();

	// RMI Shizz
	private Registry registry;
	private Master stub;
	private int port;

	private String name;
	private static int MASTER_PORT = 15214;
	private final String myHostName;

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
	public MasterImpl(List<String> allTablets, String name, String myOwnHostName) {
		this(allTablets, MASTER_PORT, name, myOwnHostName);
	}

	public MasterImpl(List<String> allTablets, int port, String name,
			String myOwnHostName) {
		// this.allWorkers = workers;
		this.allTablets = allTablets;
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
			// this.registry = LocateRegistry.createRegistry(myHostName,
			// MASTER_PORT);
			// this.registry = LocateRegistry.getRegistry(MASTER_PORT);

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

	private void resetCluster() {
		try {
			for (MapWorker mw : mapWorkers.values()) {
				mw.resetMap();
			}

			for (ReduceWorker rw : reduceWorkers.values()) {
				rw.resetReduce();
			}

			logErr("Task complete, check output.txt for results.");
		} catch (AccessException e) {
			e.printStackTrace();
			assert (false);
		} catch (RemoteException e) {
			e.printStackTrace();
			assert (false);
		}

	}

	@Override
	public synchronized void registerMapWorker(String mapWorkerName,
			MapWorker mapper) {
		if (running) {
			logErr("MapWorker [" + mapWorkerName
					+ "arrived late to the party; too bad.");
			return;
		}

		mapWorkers.put(mapWorkerName, mapper);
		List<String> tabs;
		try {
			tabs = mapper.getTablets();
			tabletsHere.addAll(tabs);
		} catch (RemoteException e) {
			e.printStackTrace();
			log("kinda expected this");
		}

		// See if we can start the map task yet.
		startIfWeCan();
	}

	@Override
	public synchronized void registerReduceWorker(String reduceWorkerName,
			ReduceWorker redder) {
		if (running) {
			logErr("ReduceWorker arrived late to the party; too bad.");
			return;
		}

		reduceWorkers.put(reduceWorkerName, redder);

		// See if we can start the map task yet.
		startIfWeCan();

	}

	/* Shared functionality between worker registration */
	private synchronized void startIfWeCan() {
		// See if we can start the map task yet.
		// wait, to see if we can get another few workers to connect
		if (running == true) {
			logErr("task already running, try again later.");
			return;
		}

		try {
			log("waiting for a sec, in case more clients want to connect...");
			Thread.sleep(200);
		} catch (InterruptedException e1) {
		}

		if (allTabletsHere() && this.map != null && this.red != null) {
			logErr("starting map task!!!!!");
			running = true;
			assignments = decideTabletAssignments();
			remainingAssignments = Collections
					.synchronizedMap(new HashMap<String, List<String>>(
							assignments));
			for (String workerName : assignments.keySet()) {
				for (String tablet : assignments.get(workerName)) {
					MapWorker w = mapWorkers.get(workerName);
					try {
						w.dispatchMapTask(this.map, tablet);
					} catch (RemoteException e) {
						e.printStackTrace();
						assert (false);
					}

				}
			}

		}

	}

	private boolean allTabletsHere() {
		// For each tablet, check that at least one mapworker has us.
		return allTablets.size() == tabletsHere.size();
	}

	@Override
	public synchronized void notifyMapDone(String workerName, String tabName) {
		logErr("Map over [" + tabName + "] completed by " + workerName);
		List<String> remainingtablets = remainingAssignments.get(workerName);
		remainingtablets.remove(tabName);

		logErr("Remaining In map Task:" + countRemaining(remainingAssignments));
		// See if we can start the reduce task yet.
		if (countRemaining(remainingAssignments) == 0) {
			reducesNotDone = new HashSet<String>(reduceWorkers.keySet());
			for (ReduceWorker worker : reduceWorkers.values()) {
				try {
					worker.dispatchReduceTask(this.red);
				} catch (RemoteException e) {
					e.printStackTrace();
					assert (false);
				}
			}
		}
	}

	private int countRemaining(Map<String, List<String>> remainingTasks) {
		int count = 0;
		for (String key : remainingTasks.keySet()) {
			List<String> vals = remainingTasks.get(key);
			count += vals.size();
		}
		return count;

	}

	@Override
	public synchronized void notifyReduceDone(String workerName) {
		logErr("Reduce completed by " + workerName);
		reducesNotDone.remove(workerName);

		if (reducesNotDone.size() == 0) {

			FileWriter writer;
			try {
				writer = new FileWriter("output.txt");
				for (Map.Entry<String, String> str : finalAnswers.entrySet()) {
					writer.write(str.getKey() + "," + str.getValue() + "\n");
				}
				writer.close();
				logErr("MAPREDUCE COMPLETED; Check output.txt for results.");
				this.resetCluster();
				this.running = false;
			} catch (IOException e) {
				e.printStackTrace();
				assert (false);
			}

		}
	}

	/**
	 * Return a mapping from workers to tablets.
	 * 
	 * @return
	 */
	private Map<String, List<String>> decideTabletAssignments() {
		// Map workers to tablets they must complete
		HashMap<String, List<String>> finalTasks = new HashMap<String, List<String>>();
		try {
			// Construct a map of Tablets to available workers
			HashMap<String, List<String>> tabsToWorkers = new HashMap<String, List<String>>();

			for (MapWorker mw : mapWorkers.values()) {
				String name = mw.getName();
				List<String> tabs = mw.getTablets();
				for (String tabName : tabs) {
					addToMap(tabsToWorkers, tabName, name);
				}
			}

			// Assign tasks stochastically
			for (Entry<String, List<String>> oneTabNWorkers : tabsToWorkers
					.entrySet()) {
				// Get the list of workers that have that tablet.
				// Choose one worker randomly to complete the map over this
				// tablet.
				List<String> possibleWorkers = oneTabNWorkers.getValue();
				String tabletName = oneTabNWorkers.getKey();
				int chosenOne = Math.abs(r.nextInt()) % possibleWorkers.size();
				String workerToComplete = possibleWorkers.get(chosenOne);
				addToMap(finalTasks, workerToComplete, tabletName);

			}

		} catch (RemoteException e) {
			e.printStackTrace();
			assert (false);
		}
		return finalTasks;
	}

	private void addToMap(Map<String, List<String>> store, String newKey,
			String newValue) {
		if (store == null) {
			store = new HashMap<String, List<String>>();
		}

		// Add to finalTasks
		if (store.get(newKey) == null) {
			List<String> values = new ArrayList<String>();
			values.add(newValue);
			store.put(newKey, values);
		} else {
			List<String> values = store.get(newKey);
			values.add(newValue);
			store.put(newKey, values);
		}
	}

	protected synchronized void log(String message) {
		System.out.printf("[%s][%s]\n", name, message);
	}

	protected synchronized void logErr(String message) {
		System.err.printf("[%s][%s]\n", name, message);
	}

	@Override
	public void run() {
		start();
	}

	@Override
	public List<ReduceWorker> getReduceWorkers() throws RemoteException {
		return new ArrayList<ReduceWorker>(this.reduceWorkers.values());
	}

	@Override
	public synchronized void recieveFinalEmit(String key, String value)
			throws RemoteException {
		finalAnswers.put(key, value);
	}

	@Override
	public void getMapReduceTasks(MapTask newMap, ReduceTask newRed)
			throws RemoteException {
		if (running) {
			logErr("Error: Task tried to be submitted while task in progress. Try again once we're done.");
			return;
		}
		this.map = newMap;
		this.red = newRed;

		startIfWeCan();
	}

}
         

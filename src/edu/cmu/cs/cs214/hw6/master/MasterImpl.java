package edu.cmu.cs.cs214.hw6.master;

import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import edu.cmu.cs.cs214.hw6.MapTask;
import edu.cmu.cs.cs214.hw6.ReduceTask;
import edu.cmu.cs.cs214.hw6.TaskFactory;
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
public class MasterImpl extends RawMaster implements Master, Runnable {

	private TaskFactory tf;
	

	private boolean running = false;
	
	// Map(workername -> tabletname)
	private List<String> mapTasks;
	private List<String> inProgressMapTasks;
	private List<String> remainingMapTasks;

	// Reduce
	private Set<String> reducesNotDone;

	// Results
	private Map<String, String> finalAnswers = new TreeMap<String, String>();

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
	public MasterImpl(String name, String myOwnHostName) {
		this(MASTER_PORT, name, myOwnHostName);
	}

	public MasterImpl(int port, String name, String myOwnHostName) {
		super(port, name, myOwnHostName);
	}

	
	@Override
	public synchronized void notifyMapDone(String workerName, String jobFileName) {
		logErr("Map over [" + jobFileName + "] completed by " + workerName);
		Runnable nextTask = remainingMapTasks.remove(0);
		inProgressMapTasks.add(nextTask);

		logErr("Remaining In map Task:" + remainingMapTasks.size() 
					+ " of " + mapTasks.size());

		
		// Give the worker another jobs.
		
		// Start Reduce Step
		if (remainingMapTasks.size() == 0 && inProgressMapTasks.size() == 0) {
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
				this.running = false;
			} catch (IOException e) {
				e.printStackTrace();
				assert (false);
			}
		}
	}
	
	
	/**
	 * Set up Master server to recieve connections from workers.
	 */
	public void start() {
		super.start();
		// Anything else before we start accepting connections?
	}




	@Override 
	public synchronized void registerMapWorker(String mapWorkerName,
			MapWorker mapper) {
		super.registerMapWorker(mapWorkerName, mapper);
		// If we're running a job, assign a task. 
	}
	
	@Override 
	public synchronized void registerReduceWorker(String reduceWorkerName,
			ReduceWorker redder) {
		super.registerReduceWorker(reduceWorkerName, redder);
		// If we're running a job, assign a task. 
	}

	
	
	/* Shared functionality between worker registration */
	public synchronized void startMRJob() {
			for (MapWorker worker : mapWorkers.values()) {
				try {
					worker.dispatchMapTask(this.map, popTask());
				} catch (RemoteException e) {
					e.printStackTrace();
					assert (false);
				}
			}
	}
	
	
	private Runnable popTask() {
		Runnable nextTaskName = remainingMapTasks.remove(0);
		inProgressMapTasks.add(nextTaskName);
		return nextTaskName;
	}
	

	@Override
	public synchronized void recieveFinalEmit(String key, String value)
			throws RemoteException {
		finalAnswers.put(key, value);
	}
	
	
	@Override
	public void run() {
		start();
	}

	/**
	 * Set task factory. Also generate tasks. Because yeah. 
	 */
	@Override
	public void setTaskFactory(TaskFactory tf, List<String> filenames)
			throws RemoteException {
		if (running) {
			logErr("Error: Task tried to be submitted while task in progress. Try again once we're done.");
			return;
		}
		this.tf = tf;
		
		mapTasks = new ArrayList<String>();
		remainingMapTasks = new ArrayList<String>();
		inProgressMapTasks = new ArrayList<String>();		
		for(String fn : filenames) {
			// Generate a maptask configured for the given filename.
			Runnable newMapTask = tf.getConfiguredMapTask(fn);
			remainingMapTasks.add(newMapTask);
		}
	}

}
         

package edu.cmu.cs.cs214.hw6.master;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import edu.cmu.cs.cs214.hw6.MapTask;
import edu.cmu.cs.cs214.hw6.ReduceTask;
import edu.cmu.cs.cs214.hw6.TaskFactory;
import edu.cmu.cs.cs214.hw6.worker.MapWorker;
import edu.cmu.cs.cs214.hw6.worker.ReduceWorker;

public interface Master extends Remote {
	
	/**
	 * 
	 * @param mapWorkerName
	 * @throws RemoteException
	 */
	public void registerMapWorker(String mapWorkerName, MapWorker mw) throws RemoteException;
	
	/**
	 * Register the name of a ReduceWorker with the Master server. 
	 * @param reduceWorkerName
	 * @throws RemoteException
	 */
	public void registerReduceWorker(String reduceWorkerName, ReduceWorker rw) throws RemoteException;
	
	/**
	 * Let a MapWorker notify the Master that their mapTask is complete.
	 * @param workerName
	 * @param tabName
	 * @throws RemoteException
	 */
	public void notifyMapDone(String workerName, String tabName) throws RemoteException;
	
	/**
	 * Let a ReduceWorker notify the Master that their reduceTask is complete. 
	 * @param workerName
	 * @throws RemoteException
	 */
	public void notifyReduceDone(String workerName) throws RemoteException;
	
	/**
	 * Return a current list of online reduce workers. 
	 * @return
	 * @throws RemoteException
	 */
	public List<ReduceWorker> getReduceWorkers() throws RemoteException;
	
	/**
	 * Collect the final values, before writing them to a file. 
	 * @param key
	 * @param value
	 * @throws RemoteException
	 */
	public void recieveFinalEmit(String key, String value) throws RemoteException;
	
	
	/**
	 * Submit tasks to call on cluster. 
	 * 
	 * Any tasks submitted when a task is running
	 * will be rudely ignored. 
	 * @param map MapTask to run on the cluster
	 * @param red ReduceTask to run on the cluster
	 * @throws RemoteException
	 */
	public void setTaskFactory(TaskFactory tf, List<String> filenames) throws RemoteException;
	
	public void startMRJob() throws RemoteException;
	
}

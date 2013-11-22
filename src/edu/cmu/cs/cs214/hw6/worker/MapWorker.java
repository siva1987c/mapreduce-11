package edu.cmu.cs.cs214.hw6.worker;

import java.rmi.RemoteException;
import java.util.List;

import edu.cmu.cs.cs214.hw6.MapTask;

public interface MapWorker extends Worker {
	
	/**
	 * Launch a reduce task over the given tablet
	 * 
	 * REQUIRES: tablet of tabletName must be held by worker
	 * 
	 * @param mt
	 * @param tabletName
	 * @throws RemoteException
	 */
	void dispatchMapTask(MapTask mt, String taskName) throws RemoteException;

		
	/**
	 * Stop a running map task if there is a map task running
	 * Otherwise, clear state and prepare for another computation.
	 * Reset state of the MapWorker to starting state.  
	 * @throws RemoteException
	 */
	void resetMap() throws RemoteException;
	
}

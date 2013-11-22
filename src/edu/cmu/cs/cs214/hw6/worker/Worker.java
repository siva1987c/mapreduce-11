package edu.cmu.cs.cs214.hw6.worker;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Interface deployed on a worker.
 * @author msebek
 *
 */
public interface Worker extends Remote {
	
	/**
	 * Determine if worker is map worker or reduce worker
	 * @return
	 * @throws RemoteException
	 */
	String MapOrReduceWorker() throws RemoteException;
	
	/**
	 * Return the name of the worker
	 * @return Assigned worker name
	 * @throws RemoteException
	 */
	String getName() throws RemoteException;

	/**
	 * Shutdown remote worker cleanly
	 * @throws RemoteException
	 */
	void shutdownWorker() throws RemoteException;
	

	boolean checkUp() throws RemoteException;
}

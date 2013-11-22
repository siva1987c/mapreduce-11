package edu.cmu.cs.cs214.hw6.worker;

import java.rmi.RemoteException;

import edu.cmu.cs.cs214.hw6.ReduceTask;

public interface ReduceWorker extends Worker {

	/**
	 * Receive an emitted value from an emitter
	 * 
	 * Note: If i had another week, and was worried about thrashing
	 * the network slightly less, I would gather these on the MapWorker, 
	 * and then send them all at once to the appropriate ReduceWorker. 
	 * @param key
	 * @param value
	 * @throws RemoteException
	 */
	void getEmit(String key, String value) throws RemoteException;

	/**
	 * Once the map task and the shuffle have been completed, 
	 * start the reduce task over a given tablet.  
	 * 
	 * @param mt
	 * @throws RemoteException
	 */
	void dispatchReduceTask(ReduceTask mt) throws RemoteException;
	
	/**
	 * When this call is received, the reduceWorker will toss
	 * the values it has accumulated, and start afresh. 
	 * This will only be used when a reduce worker
	 * dies in an extremely awkward fashion.
	 * @throws RemoteException
	 */
	void resetReduce() throws RemoteException;
}

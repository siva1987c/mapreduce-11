package edu.cmu.cs.cs214.hw6.worker;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.ReduceTask;
import edu.cmu.cs.cs214.hw6.emitter.MasterEmitter;
import edu.cmu.cs.cs214.hw6.master.Master;

/**
 * Set up a server port to recieve 
 * @author msebek
 *
 */
public class ReduceWorkerImpl extends GenericWorker implements ReduceWorker {
	    
    private int servePort;
    private HashMap<String, LinkedList<String>> emissions = new HashMap<String, LinkedList<String>>();
    
    
    private List<Thread> runningThreads = Collections.synchronizedList(new ArrayList<Thread>());
    
    public ReduceWorkerImpl(String masterHostName, int myServePort, String name) {
    	super(masterHostName);
		this.servePort = myServePort;
		this.name = name;
		
	}
    
	@Override
	public synchronized void getEmit(String key, String value) {
	// Grab the hashmap, and add it if we can. 
		List<String> row = emissions.get(key);
		if(row == null) {
			LinkedList<String> newValues = new LinkedList<String>();
			newValues.add(value);
			emissions.put(key, newValues);
		} else {
			row.add(value);
		}
	}
    
	
	
	@Override
	public synchronized void dispatchReduceTask(ReduceTask rt) throws RemoteException {
		Emitter emitter = new MasterEmitter(remoteRegistry);
		DoReduceTask redder = new DoReduceTask(rt, emitter);
		Thread t1 = new Thread(redder);
		t1.start();
		log("Reduce Task dispatched");

	}
	
	class DoReduceTask implements Runnable {
		private ReduceTask rt;
		// This emitter emits back to master. 
		private Emitter em;
		DoReduceTask(ReduceTask task, Emitter emitter) {
			rt = task;
			em = emitter;
		}
		
		@Override
		public void run() {
			log("Reducer started");
			// Perform Reduce task
			for(String key : emissions.keySet()) {
				LinkedList<String> values = emissions.get(key);
				rt.execute(key, values.iterator(), em);
				
			}
				
			
			log("Reduce Task complete.");
			try {
				remoteMaster.notifyReduceDone(name);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				assert(false);
			}
		}
	}

	
	@Override
	public void registerWorker(Master remoteMaster) throws RemoteException {
		remoteMaster.registerReduceWorker(name, this);
	}
	
	@Override
	public int getObjectServePort() {
		return this.servePort;
	}

    @Override
	public String MapOrReduceWorker() throws RemoteException {
    	return "reduce";
	}
	@Override
	public String getName() throws RemoteException {
		return this.name;
	}

	@Override
	public void resetReduce() throws RemoteException {
		// Reset lists.
		emissions = new HashMap<String, LinkedList<String>>();
		
		for(Thread t: runningThreads) {
			t.interrupt();
	
		}
		runningThreads = Collections.synchronizedList(new ArrayList<Thread>()); 
		
	}

    
}
    
    
		
	
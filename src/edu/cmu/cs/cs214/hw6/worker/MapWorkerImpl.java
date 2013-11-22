package edu.cmu.cs.cs214.hw6.worker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.MapTask;
import edu.cmu.cs.cs214.hw6.Tablet;
import edu.cmu.cs.cs214.hw6.emitter.ShuffleEmitter;
import edu.cmu.cs.cs214.hw6.master.Master;

/**
 * MapWorker 
 * 
 * Responsibilities:
 * Receive the map/reduce task.
 * Perform actual map/reduce
 * 
 * 1. Connect to MasterServer
 * 2. [ inform master what tablets it holds ]
 * 3. Receive Map task; run it. 
 * 4. Receive Reduce task; run it. 
 * Notify MasterServer on completion. 
 * @author msebek
 *
 */
public class MapWorkerImpl extends GenericWorker implements MapWorker {
	
    private int servePort;
   
    private List<Thread> runningThreads = Collections.synchronizedList(new ArrayList<Thread>());
    
	public MapWorkerImpl(String masterHostname, int myServePort, String name) {
		super(masterHostname);
		this.servePort = myServePort;
		this.name = name;
	}	
	
	public void dispatchMapTask(MapTask r) throws RemoteException {
		// Set up the emitter.
		List<ReduceWorker> reduceWorkers = remoteMaster.getReduceWorkers();
		Emitter emitter = new ShuffleEmitter(reduceWorkers);
		
		//DoMapTask mapper = new DoMapTask(task, emitter, tabletName);
		r.configureEmitter(emitter);
		Thread t1 = new Thread((Runnable)r);
		t1.start();
		runningThreads.add(t1);
		log("Map Task dispatched");
		// Inform server map task complete. 
	}
	
	/**
	 * Runnable to perform actual mapping, in a non-blocking
	 * fashion.
	 * @author msebek
	 *
	 */
	/*class DoMapTask implements Runnable {
		private MapTask mt;
		private Emitter em;
		private String tab;
		DoMapTask(MapTask task, Emitter emitter, String tabletName) {
			mt = task;
			em = emitter;
			tab = tabletName;
		}
		
		@Override
		public void run() {
			log("Mapper started");
			Tablet t = getTablet(tab);
			Iterator<File> iter = t.iterator();
			while(iter.hasNext()) {
				File f = iter.next();
				InputStream input = null;
				try {
					input = new FileInputStream(f);
					// Perform Map task
					mt.execute(f.getName(), input, em);
					input.close();
				
				} catch (FileNotFoundException e) {
					logErr("map file not found, this shouldn't happen");
					e.printStackTrace();
					assert(false);
				} catch (IOException e) {
					e.printStackTrace();
					assert(false);
				}
			
			}
			
			
			try {
				remoteMaster.notifyMapDone(name, tab);
			} catch (RemoteException e) {
				e.printStackTrace();
				assert(false);
			}
			
			// Final things
			log("Map Task complete.");
			runningThreads.remove(this);
		}
		
		private Tablet getTablet(String tab) {
			for(Tablet t:tablets) {
				if(t.getName().equals(tab))
					return t;
			}
			assert(false);
			return null;
		}
	}*/

	@Override
	public String MapOrReduceWorker() throws RemoteException {
		return "map";
	}


	@Override
	public String getName() {
		return this.name;
	}


	@Override
	public synchronized void registerWorker(Master remoteMaster) throws RemoteException {
		remoteMaster.registerMapWorker(name, this);
	}


	@Override
	public int getObjectServePort() {
		return this.servePort;
	}

	@Override
	public void resetMap() throws RemoteException {
		// Join any thread that's outstanding.
		for(Thread t : runningThreads) {
				t.interrupt();
		}
	}
	
}

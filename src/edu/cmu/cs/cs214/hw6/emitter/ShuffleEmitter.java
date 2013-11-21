package edu.cmu.cs.cs214.hw6.emitter;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.worker.ReduceWorker;

/**
 * Shuffle Emitter emits the key-value pairs to the correct reduce worker. 
 * @author msebek
 *
 */
public class ShuffleEmitter implements Emitter {

	private List<ReduceWorker> reducers = new ArrayList<ReduceWorker>();
	private Random r = new Random(); 
	
	public ShuffleEmitter(List<ReduceWorker> reducers) {
		this.reducers = reducers;
	}
	

	/**
	 * Emit to Reduce worker based on hash code of key
	 */
	@Override
	public void emit(String key, String value) {
		
		int hashed = Math.abs(key.hashCode());
		int whichReducer = hashed % reducers.size();
		
		ReduceWorker rw = reducers.get(whichReducer);
		
		try {
			rw.getEmit(key, value);
		} catch (RemoteException e) {
			e.printStackTrace();
			assert(false);
		}
		
		if(r.nextDouble() < .005 )
			System.out.printf(".");
		
	}

}

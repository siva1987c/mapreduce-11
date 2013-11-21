package edu.cmu.cs.cs214.hw6.emitter;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Random;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.master.Master;

public class MasterEmitter implements Emitter {

	private Master master;
	private Random r = new Random(); 
	
	public MasterEmitter(Registry r) {
		try {
			master = (Master) r.lookup("master");
		} catch (AccessException e) {
			e.printStackTrace();
			assert(false);
		} catch (RemoteException e) {
			e.printStackTrace();
			assert(false);
		} catch (NotBoundException e) {
			e.printStackTrace();
			assert(false);
		}
	}
	

	/**
	 * Emit to Reduce worker based on hash code of key
	 */
	@Override
	public void emit(String key, String value) {
		
		try {
			master.recieveFinalEmit(key, value);
		} catch (RemoteException e) {
			e.printStackTrace();
			assert(false);
		}
		
		if(r.nextDouble() < .07 )
			System.out.printf(".");
		
	}

}

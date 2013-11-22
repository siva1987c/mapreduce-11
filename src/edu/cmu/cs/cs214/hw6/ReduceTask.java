package edu.cmu.cs.cs214.hw6;

import java.util.Iterator;

/**
 * A plug-in interface for the reduce portion of a map/reduce computation.
 */
public interface ReduceTask {

	/**
	 * Given a key and the results emitted for that key by the map portion of this
	 * map/reduce computation, outputs the final key/value results for the computation
	 * to the framework using the <code>Emitter</code>.
	 * 
	 * @param key The key currently being reduced by this task.
	 * @param values An iterator of all the values emitted for this key by the map portion
	 * of the map/reduce computation.
	 * @param emitter The <code>Emitter</code> being used to communicate the final results
	 * of this computation to the map/reduce framework.
	 */
	public void configure(String key, Iterator<String> values, Emitter emitter);
	
	public void run();
}

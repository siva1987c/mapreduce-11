package edu.cmu.cs.cs214.hw6;

import java.io.InputStream;

/**
 * A plug-in interface for the map portion of a map/reduce computation.
 * NOTE: MapTask and ReduceTask must implement clone() 
 */
public interface MapTask {
	/**
	 * Returns the name of this task.  Your framework may use this name (or not use this
	 * name) however you want.
	 * @return The name of this task.
	 */
	public String getName();
	
	/**
	 * Given a document and its contents, executes the map portion of a map/reduce computation
	 * over that single document, outputting intermediate key/value pair results to the 
	 * framework using the <code>Emitter</code>.
	 * 
	 * @param documentName The name of the document currently being analyzed by this task.
	 * @param documentContents The contents of the document currently being analyzed by this task.
	 * @param emitter The <code>Emitter</code> being used to communicate intermediate results
	 * to the map/reduce framework.
	 */
	public void execute(String documentName, InputStream documentContents, Emitter emitter);
	
}

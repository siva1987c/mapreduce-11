package edu.cmu.cs.cs214.hw6;


/**
 * A plug-in interface for the map portion of a map/reduce computation.
 */
public interface MapTask {

	/**
	 * Must be called before run is called.
	 * @param emitter The <code>Emitter</code> being used to communicate intermediate results
	 * to the map/reduce framework.
	 */
	public void configureEmitter(Emitter emitter);

	public void run();
}

package edu.cmu.cs.cs214.hw6;

/**
 * Provides an interface to allow a <code>MapTask</code> or <code>ReduceTask</code>
 * to output a key/value pair as a result to the framework.
 */
public interface Emitter {
	/**
	 * Outputs a key/value pair as a result to the framework.
	 * @param key  The key to output to the framework.
	 * @param value The value to output to the framework.
	 */
	public void emit(String key, String value);
}

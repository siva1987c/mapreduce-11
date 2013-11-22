package edu.cmu.cs.cs214.hw6;

public interface TaskFactory {

	public MapTaskRunnable getConfiguredMapTask(String config);
	
	public Runnable getConfiguredReduceTask(String config);
}

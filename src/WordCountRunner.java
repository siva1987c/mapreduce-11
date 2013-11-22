import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;

import edu.cmu.cs.cs214.hw6.master.Master;
import edu.cmu.cs.cs214.hw6.master.MasterImpl;
import edu.cmu.cs.cs214.hw6.tasks.WordCountTaskFactory;
import edu.cmu.cs.cs214.hw6.worker.MapWorkerImpl;
import edu.cmu.cs.cs214.hw6.worker.ReduceWorkerImpl;

/**
 * Submits a WordCount task to the cluster.
 * 
 * @author msebek
 * 
 */
public class WordCountRunner {

	private Registry registry;
	private Master remoteMaster;
	private int DEFAULT_MASTER_PORT = 15214;

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out
					.println("Usage: java WordCount " +
							"128.123.123.456 (the IP/hostname of the master server)");
			System.out.println("Defaulting to localhost...");
			new WordCountRunner("localhost");
			return;
		} else if(args.length == 1) {
			System.out.printf("Submitting task to [%s]", args[0]);
			new WordCountRunner(args[0]);
		}
	}

	public WordCountRunner(String masterHostName) {
		// Grab the master
		// Generate Runnables?
		// Connect to Master.
		while (true) {
			try {
				// Start the master
				MasterImpl master = new MasterImpl("Gojira", masterHostName);
				new Thread(master).run();
				System.out.printf("Starting master...");
				Thread.sleep(2000);
				// Add self to registry
				registry = LocateRegistry.getRegistry(masterHostName,
						DEFAULT_MASTER_PORT);
				// Get the Master object
				remoteMaster = (Master) registry.lookup("master");

				// Produce map/reduce tasks
				WordCountTaskFactory mrfact = new WordCountTaskFactory();
				
				
				// Make a list of filenames to run against
				String[] inputFiles = {"C:\\workspace\\MapReduce\\assets\\tablets\\pg1.txt",
				                       "C:\\workspace\\MapReduce\\assets\\tablets\\pg2.txt"};
				remoteMaster.setTaskFactory(mrfact, Arrays.asList(inputFiles));
				
				// Start up mapper and reducer
				MapWorkerImpl osaka = new MapWorkerImpl(masterHostName, 15110, "mapper1");
				ReduceWorkerImpl chiba = new ReduceWorkerImpl(masterHostName, 18213, "reducer1");
				
				
				// Spin up Map workers
				new Thread(osaka).run();
				// Spin up Reduce workers
				new Thread(chiba).start();
				
				System.out.println("Workers started...");
				Thread.sleep(5000);
				remoteMaster.startMRJob();
				System.out.println("Task submitted to master.");
				break;
			} catch (IOException e1) {
				System.out.println("Could not connect to " + masterHostName + ", port "
						+ DEFAULT_MASTER_PORT + " Retrying...");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
				}
			} catch (NotBoundException e) {
				System.out.println("Server not online yet:" + masterHostName + ", port "
						+ DEFAULT_MASTER_PORT + " Retrying...");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException ie) {
				} finally {
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				assert(false);
			}
		}
	}
}


import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import edu.cmu.cs.cs214.hw6.MapTask;
import edu.cmu.cs.cs214.hw6.ReduceTask;
import edu.cmu.cs.cs214.hw6.master.Master;
import edu.cmu.cs.cs214.hw6.tasks.RisklessMapTask;
import edu.cmu.cs.cs214.hw6.tasks.RisklessReduceTask;

/**
 * Submits a WordCount task to the cluster.
 * 
 * @author msebek
 * 
 */
public class RisklessTask {

	private Registry registry;
	private Master remoteMaster;
	private int DEFAULT_MASTER_PORT = 15214;

	public static void main(String[] args) {
		if (args.length == 0) {
			System.out
					.println("Usage: java RisklessTask " +
							"128.123.123.456 (the IP/hostname of the master server)");
			System.out.println("Defaulting to localhost...");
			new RisklessTask("localhost");
			return;
		} else if(args.length == 1) {
			System.out.printf("Submitting task to [%s]", args[0]);
			new RisklessTask(args[0]);
		}
	}

	public RisklessTask(String masterHostName) {
		while (true) {
			try {
				// Add self to registry
				registry = LocateRegistry.getRegistry(masterHostName,
						DEFAULT_MASTER_PORT);
				// Get the Master object
				remoteMaster = (Master) registry.lookup("master");

				// Set Up MapReduce Tasks
				MapTask map = new RisklessMapTask();
				ReduceTask red = new RisklessReduceTask();
				remoteMaster.setMapReduceTasks(map, red);

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
			}
		}
	}
}

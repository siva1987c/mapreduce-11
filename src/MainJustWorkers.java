import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import edu.cmu.cs.cs214.hw6.worker.MapWorkerImpl;
import edu.cmu.cs.cs214.hw6.worker.ReduceWorkerImpl;

/**
 * Requested File #2: Spin up just workers.  
 *
 * 
 * @author msebek
 *
 */
public class MainJustWorkers {

	public static void main(String[] args) throws IOException {
		
		String masterHostName = "128.237.240.187";//"abel.wv.cc.cmu.edu";
		if(args.length == 0) {
			System.out.printf("Starting with default configuration...n");
			System.out.printf("Spinning up MapWorkers 'Tokyo', 'Kyoto', and ReduceWorker 'Gifu'\n");
			System.out.printf("Tablets: {2, 5, 8}, {3, 6, 9}\n");
			System.out.printf("Host:localhost\n");
			System.out.printf("Normal usage: java MainJustWorkers\n");
		} else if(args.length == 1) {
			masterHostName = args[0];
			System.out.println("Starting default configuration, but new host.");
			System.out.printf("Searching for host at [%s]", masterHostName);
		} else if(args.length == 4) {
			// Note: spin up one map worker and one reduce worker per machine this is run on. 
			System.out.printf("Matched 'java MainJustWorkers uniqueServerName uniqueServePort masterHostname  \"1,2,3\" (list of tablets)\n");
			System.out.printf("Assuming localhost as master location.\n");
			String name = args[0];
			int servePort = Integer.parseInt(args[1]);
			String hostname = args[2];
			String[] tablets = args[3].split(",");
			// Start it up
			MapWorkerImpl mapper = new MapWorkerImpl(hostname, servePort, name+"Map", Arrays.asList(tablets));
			new Thread(mapper).start();	
			ReduceWorkerImpl reducer = new ReduceWorkerImpl(hostname, servePort, name+"Reduce");
			new Thread(reducer).start();
			return;
			
			
		}
		
		
		/**
		 * Note: Feel free to not spin up a server or two. 
		 *       As long as all of the tablets are available, 
		 *       the map task will proceed. 
		 */
		
		
		// Map Workers
		String[] toky = {"2", "5", "8"};
		ArrayList<String> tokyoTabs = new ArrayList<String>(Arrays.asList(toky));
		MapWorkerImpl tokyo = new MapWorkerImpl(masterHostName, 15112, "Tokyo", tokyoTabs);
		
		String[] kyot = {"3", "6", "9"};
		ArrayList<String> kyotoTabs = new ArrayList<String>(Arrays.asList(kyot));
		MapWorkerImpl kyoto = new MapWorkerImpl(masterHostName, 15122, "Kyoto", kyotoTabs);

		// Reduce Workers
		ReduceWorkerImpl gifu = new ReduceWorkerImpl(masterHostName, 18240, "Gifu");

		
		// Server		
		// Six map, three reduce (all 10 tablets with extra)
		String[] workarr = {"Osaka", "Tokyo", "Kyoto", "Nagasaki", "Sapporo", "Nagoya", "Chiba", "Gifu", "Hiroshima"};
		
		// Spin up Map workers
		new Thread(tokyo).start();
		new Thread(kyoto).start();
		
		// Spin up Reduce workers
		new Thread(gifu).start();
		
		
    }

	
}

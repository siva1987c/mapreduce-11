import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import edu.cmu.cs.cs214.hw6.MapTask;
import edu.cmu.cs.cs214.hw6.ReduceTask;
import edu.cmu.cs.cs214.hw6.master.MasterImpl;
import edu.cmu.cs.cs214.hw6.tasks.PrefixGuessMapTask;
import edu.cmu.cs.cs214.hw6.tasks.PrefixGuessReduceTask;
import edu.cmu.cs.cs214.hw6.worker.MapWorkerImpl;
import edu.cmu.cs.cs214.hw6.worker.ReduceWorkerImpl;

/**
 * Requested File #1: Spin up just a server
 * 
 * But I'm also going to spin up a map worker and a reduce worker, 
 * because that will make things less boring. 
 * @author msebek
 *
 */
public class MainJustServer {

	public static void main(String[] args) throws IOException {
		
		String masterHostName = "128.237.240.187"; // "abel.wv.cc.cmu.edu";
		
		if(args.length == 0) {
			System.out.printf("Using default arguments...\n");
			System.out.printf("Spinning up Master 'Gojira', MapWorker 'Osaka', and ReduceWorker 'Chiba'\n");
			System.out.printf("Tablets: 1, 4, 7, 10.\n");
			System.out.printf("Host:[%s]\n", masterHostName);
			System.out.printf("Normal usage: java MainJustServer\n");
		} else if(args.length == 1) {
			System.out.printf("Spinning up Master 'Gojira', MapWorker 'Osaka', and ReduceWorker 'Chiba'\n");
			System.out.printf("Tablets: 1, 4, 7, 10.\n");
			System.out.printf("Host[%s]\n", args[0]);
			masterHostName = args[0];
			System.out.printf("Normal usage: java MainJustServer\n");
		} else 
			{ System.out.printf("Normal usage: java MainJustServer"); return;}
		
		
		// Map Workers
		String[] osak = {"1", "4", "7", "10"};
		ArrayList<String> osakaTabs = new ArrayList<String>(Arrays.asList(osak));
		MapWorkerImpl osaka = new MapWorkerImpl(masterHostName, 15110, "Osaka", osakaTabs);
		
		
		// Reduce Workers
		ReduceWorkerImpl chiba = new ReduceWorkerImpl(masterHostName, 18213, "Chiba");
		

		// Server		
		String[] allTablet = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
		ArrayList<String> allTablets = new ArrayList<String>(Arrays.asList(allTablet));
		
		MasterImpl master = new MasterImpl(allTablets, "Gojira", masterHostName);
		
		// Spin up Gojira
		new Thread(master).run();
		
		
		// Spin up Map workers
		new Thread(osaka).run();
		// Spin up Reduce workers
		new Thread(chiba).start();
		
		
    }

	
}

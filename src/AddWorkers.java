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
public class AddWorkers {

	public static void main(String[] args) throws IOException {
		
		String masterHostName = "128.237.240.187";//"abel.wv.cc.cmu.edu";
		int servePort = 15214;
		if(args.length == 0) {
			System.out.printf("Starting with default configuration...n");
			System.out.printf("Host:localhost\n");
			System.out.printf("Normal usage: java AddMapWorker 5 123.123.123.123\n");
			System.out.printf("Normal usage: java AddMapWorker (numMapperInstances) (masterIPAddress)\n");
		} else if(args.length == 2) {
			// Note: spin up one map worker and one reduce worker per machine this is run on. 
			String name = args[0];
			int numMappers = Integer.parseInt(args[1]);
			String hostname = args[2];
			// Start up a Mapper Thread and a Reducer thread
			MapWorkerImpl mapper = new MapWorkerImpl(hostname, servePort, name+"Map");
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
		MapWorkerImpl tokyo = new MapWorkerImpl(masterHostName, 15112, "Tokyo");
		MapWorkerImpl kyoto = new MapWorkerImpl(masterHostName, 15122, "Kyoto");

		// Reduce Workers
		ReduceWorkerImpl gifu = new ReduceWorkerImpl(masterHostName, 18240, "Gifu");

		
		// Spin up Map workers
		new Thread(tokyo).start();
		new Thread(kyoto).start();
		
		// Spin up Reduce workers
		new Thread(gifu).start();
		
		
		
    }

	
}

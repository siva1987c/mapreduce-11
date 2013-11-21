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
 * So, instead of making you press the run buttons a bunch
 * of times, this seemed both more reasonable and more 
 * controlled. 
 * 
 * To run, press run. 
 * 
 * MainMain will spawn a bunch of map workers, reduce workers, 
 * and a master server. 
 * 
 * @author msebek
 *
 */
public class MainMain {

	public static void main(String[] args) throws IOException {
		
		String masterHostName = "128.237.240.187";//"abel.wv.cc.cmu.edu";
		/**
		 * Note: Feel free to not spin up a server or two. 
		 *       As long as all of the tablets are available, 
		 *       the map task will proceed. 
		 */
		
		
		// Map Workers
		String[] osak = {"1", "4", "7", "10"};
		ArrayList<String> osakaTabs = new ArrayList<String>(Arrays.asList(osak));
		MapWorkerImpl osaka = new MapWorkerImpl(masterHostName, 15110, "Osaka", osakaTabs);
		
		
		String[] toky = {"2", "5", "8"};
		ArrayList<String> tokyoTabs = new ArrayList<String>(Arrays.asList(toky));
		MapWorkerImpl tokyo = new MapWorkerImpl(masterHostName, 15112, "Tokyo", tokyoTabs);
		
		String[] kyot = {"3", "6", "9"};
		ArrayList<String> kyotoTabs = new ArrayList<String>(Arrays.asList(kyot));
		MapWorkerImpl kyoto = new MapWorkerImpl(masterHostName, 15122, "Kyoto", kyotoTabs);

		// More Map workers
		String[] nagasak = {"1", "2", "3", "4"};
		ArrayList<String> nagasakiTabs = new ArrayList<String>(Arrays.asList(nagasak));
		MapWorkerImpl nagasaki = new MapWorkerImpl(masterHostName, 15150, "Nagasaki", nagasakiTabs);
		
		
		// it was right around here that I really started struggling with city names
		String[] sappor = {"5", "6", "7"};
		ArrayList<String> sapporoTabs = new ArrayList<String>(Arrays.asList(sappor));
		MapWorkerImpl sapporo = new MapWorkerImpl(masterHostName, 15323, "Sapporo", sapporoTabs);
		
		String[] nagoy = {"8", "9", "10"};
		ArrayList<String> nagoyaTabs = new ArrayList<String>(Arrays.asList(nagoy));
		MapWorkerImpl nagoya = new MapWorkerImpl(masterHostName, 15440, "Nagoya", nagoyaTabs);

		
		
		// Reduce Workers
		ReduceWorkerImpl chiba = new ReduceWorkerImpl(masterHostName, 18213, "Chiba");
		
		ReduceWorkerImpl gifu = new ReduceWorkerImpl(masterHostName, 18240, "Gifu");
		
		ReduceWorkerImpl hiroshima = new ReduceWorkerImpl(masterHostName, 18290, "Hiroshima");

		
		// Server		
		// Six map, three reduce (all 10 tablets with extra)
		String[] workarr = {"Osaka", "Tokyo", "Kyoto", "Nagasaki", "Sapporo", "Nagoya", "Chiba", "Gifu", "Hiroshima"};
		
		
		String[] allTablet = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
		ArrayList<String> allTablets = new ArrayList<String>(Arrays.asList(allTablet));
		
		MapTask map = new PrefixGuessMapTask();
		ReduceTask red = new PrefixGuessReduceTask(); 
		MasterImpl master = new MasterImpl(allTablets, "Gojira", masterHostName);
		
		// Spin up Gojira
		new Thread(master).start();
		
		
		// Spin up Map workers
		new Thread(osaka).start();
		new Thread(tokyo).start();
		new Thread(kyoto).start();
		new Thread(nagasaki).start();
		new Thread(sapporo).start();
		new Thread(nagoya).start();
		
		
		// Spin up Reduce workers
		new Thread(chiba).start();
		new Thread(gifu).start();
		new Thread(hiroshima).start();
		
		
    }

	
}

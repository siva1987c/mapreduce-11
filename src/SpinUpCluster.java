import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Submits a WordCount task to the cluster.
 * 
 * @author msebek
 * 
 */
public class SpinUpCluster {

	private String[] formatStrings = 
		{ "ghc%d.ghc.andrew.cmu.edu",
		  "unix%d.andrew.cmu.edu"};
	private int[] machineLowNumber = {1, 1};
	private int[] machineHighNumber = {79, 6};
	// Note: We will have to worry about the "allow key?" bit.
	// TODO: find flag to turn off warning
	// TODO: make mappers exit after 30 seconds without master connectivity
	
	public static void main(String[] args) {
		if (args.length == 0) {
			System.out
					.println("Usage: java SpinUpCluster " +
							"128.123.123.456 (the IP/hostname of the master server)");
			return;
		} else if(args.length == 1) {
			System.out.printf("Adding machines to cluster...", args[0]);
			launchMapper("unix5.andrew.cmu.edu");
		}
	}
	
	/**
	 * Return the first n host names
	 * @param numMachines
	 */
	private static List<String> chooseHostNames(int numMachines) {
		ArrayList<String> hosts = new ArrayList<String>();
		// TODO: make this choose them randomly. 
		// TODO Later: Also round-robin a port.
		for(int i =0; i < numMachines; i ++) {
			hosts.add(getHostName(i));
		}
		return hosts;
	}
	
	private static String getHostName(int i) {
		assert(i < 0);
		if(i < 80) {
			return String.format("ghc%d.ghc.andrew.cmu.edu", i);
		} else if (i < 87) {
			return String.format("unix%d.andrew.cmu.edu", i-80);
		} 
		assert(false);
		return "";
	}
	
	/**
	 * Uses kerberos to log into another machine, and start an instance.
	 * @requires you're authed with a unix machine already
	 * @param hostname
	 */
	private static void launchMapper(String hostname) {
		System.out.println("Starting mapper...\n");
		// TODO: Do Thing with Niceness.
		String sshString = String.format("ssh -K -o StrictHostKeyChecking=no %s", hostname);
		Runtime r = Runtime.getRuntime();
		Process p = null;
		try {
			p = r.exec(sshString);
			Thread.sleep(2000);
		} catch (IOException e) {
			e.printStackTrace();
			assert(false);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
		// Code blob from Stackoverflow.
		PrintWriter out = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(p.getOutputStream())), true);

		// We are now logged into the host. 
		out.println("java GodKnowsWhat ???");
		try {
			Thread.sleep(1000);
			while (in.ready()) {
			  String s = in.readLine();
			  System.out.println(s);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assert(false);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assert(false);
		}
		
		// Close out ssh session
		out.println("exit\n");
		
	}
	
	
	
}

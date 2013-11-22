import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

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
					.println("Usage: java SpinUpCluster" +
							"128.123.123.456 (the IP/hostname of the master server)");
			System.out.println("Defaulting to localhost...");
			return;
		} else if(args.length == 1) {
			System.out.printf("Adding machines to cluster...", args[0]);
			launchMapper("unix5.andrew.cmu.edu");
		}
	}
	/**
	 * Uses kerberos to log into another machine, and start an instance.
	 * @requires you're authed with a unix machine already
	 * @param hostname
	 */
	private static void launchMapper(String hostname) {
		String sshString = String.format("ssh -K %s", hostname);
		Runtime r = Runtime.getRuntime();
		Process p = null;
		try {
			p = r.exec(sshString);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assert(false);
		}
		PrintStream out = new PrintStream(p.getOutputStream());
		BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));

		// We are now logged into the host. 
		out.println("ls ~");
		try {
			while (in.ready()) {
			  String s = in.readLine();
			  System.out.println(s);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			assert(false);
		}
		out.println("exit");
		
	}
	
	
	
}

package edu.cmu.cs.cs214.hw6.tasks;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Scanner;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.MapTask;

/**
 * The map portion of a word-counting map/reduce computation.  For each occurrence
 * of each word in a corpus of data, the map task will emit a key/value pair allowing
 * that occurrence to be counted by the reduce portion of the computation. 
 */

public class RisklessMapTask implements MapTask, Serializable{
	private static final long serialVersionUID = 2865319920651080365L;

	@Override
	public void execute(String documentName, InputStream documentContents,
			Emitter emitter) {
		Scanner scanner = new Scanner(documentContents);
		scanner.useDelimiter("\\W+");
		while (scanner.hasNext()) {
			String word = scanner.next().trim().toLowerCase();
			emitter.emit(word, "1");
		}
		scanner.close();
	}

	@Override
	public String getName() {
		return "Riskless";
	}

}

package edu.cmu.cs.cs214.hw6.tasks;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Scanner;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.MapTask;

/**
 * The map portion of the prefix-best-guess map/reduce computation.  For each occurrence
 * of each word in a corpus of data, the map task will emit a key/value pair allowing
 * that occurrence to be counted by the reduce portion of the computation. 
 */
public class PrefixGuessMapTask implements MapTask, Serializable {
	private static final long serialVersionUID = 2665319920651030365L;
	
	@Override
	public void execute(Emitter emitter) {
		assert(false);
		String documentName=null;
		InputStream documentContents=null;
		Scanner scanner = new Scanner(documentContents);
		scanner.useDelimiter("\\W+");
		while (scanner.hasNext()) {
			String word = scanner.next().trim().toLowerCase();
			
			if(word.matches("^\\d+$")) continue;
			
			for(int index = 1; index <= word.length(); index++) {
				emitter.emit(word.substring(0, index), word);
			}
		}
		scanner.close();
	}

	@Override
	public String getName() {
		return "PrefixGuess";
	}

}

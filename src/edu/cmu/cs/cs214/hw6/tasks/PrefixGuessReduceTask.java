package edu.cmu.cs.cs214.hw6.tasks;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.ReduceTask;

/**
 * The reduce portion of a Prefix Guess map/reduce computation.  For each word observed
 * in a corpus of data (during the map portion of the computation) this reduce task will
 * sum the number of occurrences (the values here) and output the total number of 
 * occurrences of that word in the corpus. 
 */
public class PrefixGuessReduceTask implements ReduceTask, Serializable {
	private static final long serialVersionUID = -3329813246878032491L;

	@Override
	public void execute(String key, Iterator<String> values, Emitter emitter) {
		/* key:value pairs, where key = prefix, value = word */
		Map<String, Integer> counter = new HashMap<String, Integer>();
		
		while (values.hasNext()) {
			String guess = values.next();
			if(counter.get(guess) == null) {
				counter.put(guess, 1);
			} else {
				int prev = counter.get(guess);
				counter.put(guess, prev+1);
			}
		}
		// Return the value with the most occurrences
		int highestVal = Collections.max(counter.values());
		
		// return the first in the case of a tie
		for(Entry<String, Integer> r : counter.entrySet()) {
			if(r.getValue() == highestVal) { 
				emitter.emit(key, r.getKey());
				return;
			}
		}
		assert(false);
		return;
		
	}

	@Override
	public String getName() {
		return "PrefixGuess";
	}

}

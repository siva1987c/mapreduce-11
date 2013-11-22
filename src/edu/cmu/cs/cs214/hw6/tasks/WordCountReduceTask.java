package edu.cmu.cs.cs214.hw6.tasks;

import java.io.Serializable;
import java.util.Iterator;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.ReduceTask;
// Old. Unused.
/**
 * The reduce portion of a word-counting map/reduce computation.  For each word observed
 * in a corpus of data (during the map portion of the computation) this reduce task will
 * sum the number of occurrences (the values here) and output the total number of 
 * occurrences of that word in the corpus. 
 */
public class WordCountReduceTask {//implements ReduceTask, Serializable {
	private static final long serialVersionUID = -3329813246878032491L;


	public void execute(String key, Iterator<String> values, Emitter emitter) {
		int sum = 0;
		while (values.hasNext()) {
			sum += Integer.valueOf(values.next());
		}
		emitter.emit(key, Integer.toString(sum));
	}


	public String getName() {
		return "WordCount";
	}


	public void configure(String key, Iterator<String> values, Emitter emitter) {
		// TODO Auto-generated method stub
		
	}


	public void run() {
		// TODO Auto-generated method stub
		
	}

}

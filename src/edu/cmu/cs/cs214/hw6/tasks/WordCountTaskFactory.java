package edu.cmu.cs.cs214.hw6.tasks;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Scanner;

import edu.cmu.cs.cs214.hw6.Emitter;
import edu.cmu.cs.cs214.hw6.MapTask;
import edu.cmu.cs.cs214.hw6.ReduceTask;
import edu.cmu.cs.cs214.hw6.TaskFactory;

/**
 * The map portion of a word-counting map/reduce computation.  For each occurrence
 * of each word in a corpus of data, the map task will emit a key/value pair allowing
 * that occurrence to be counted by the reduce portion of the computation. 
 */
public class WordCountTaskFactory implements TaskFactory {

	public Runnable getConfiguredMapTask(String config) {
		return new maptask(config); 
	}
	
	public Runnable getConfiguredReduceTask(String config) {
		// Config doesn't do anything...
		return new redtask();
	}
	
	
	// The Embedded Map Task
	private class maptask implements Runnable, MapTask, Serializable {
		private static final long serialVersionUID = 2865319920651030365L;
		private String documentPath;
		private Emitter emitter;
		
		
		public maptask(String config) {
			// config corresponds to a filename on the local filesystem.
			//   in this case, we'll make it the document path.
			documentPath = config;
		}
		
		@Override
		public void run() {
		
			// Open the file specified in the path.
			File file = new File(documentPath);
			FileInputStream documentContents = null;
	 
			try {
				documentContents = new FileInputStream(file);
			} catch (Exception e) { assert(false); }
			
			// Read each word from the file
			Scanner scanner = new Scanner(documentContents);
			scanner.useDelimiter("\\W+");
			while (scanner.hasNext()) {
				String word = scanner.next().trim().toLowerCase();
				emitter.emit(word, "1");
			}
			scanner.close();
			
		}
		@Override
		public void configureEmitter(Emitter emitter) {
			this.emitter = emitter;
		}
	}
	
	// The embedded reducetask
	private class redtask implements Runnable, ReduceTask, Serializable {
		private static final long serialVersionUID = -3329813246878032491L;
		private String key;
		private Iterator<String> values;
		private Emitter emitter;
		
		public void configure(String key, Iterator<String> values, Emitter emitter) {
			this.values = values;
			this.key = key;
			this.emitter = emitter;
		}

		public void run() {
			int sum = 0;
			while (values.hasNext()) {
				sum += Integer.valueOf(values.next());
			}
			emitter.emit(key, Integer.toString(sum));

		}
		
	}
}

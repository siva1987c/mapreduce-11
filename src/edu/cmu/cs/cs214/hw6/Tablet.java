package edu.cmu.cs.cs214.hw6;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Treats a directory of local files as a tablet -- a collection of files --
 * in a faux distributed storage system.  This class is a simple utility 
 * class to allow a client to iterate over the collection of files which
 * are stored at a specified location in our faux storage system.
 */
public class Tablet implements Iterable<File>, Serializable {
	private static final long serialVersionUID = 1084064013108627816L;
	private static final String ALL_TABLETS_DIRECTORY = "assets/tablets";
	private final String name;
	
	/**
	 * Given a tablet name, constructs a tablet representing that collection 
	 * of files in the faux distributed storage system.
	 * 
	 * @param name The name of the tablet.
	 */
	public Tablet(String name) {
		this.name = name;
	}
	
	/**
	 * Returns the name of the tablet in the faux distributed storage system.
	 * @return the name of the tablet in the faux distributed storage system.
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns an iterator over the files in this tablet in the faux distributed
	 * storage system.
	 * 
	 * @return An iterator over the files in this tablet.
	 */
	@Override
	public Iterator<File> iterator() {
		File allTabletsDirectory = new File(ALL_TABLETS_DIRECTORY);
		if (!allTabletsDirectory.isDirectory() || !allTabletsDirectory.canRead()) {
			throw new RuntimeException("Cannot read parent tablets directory.");
		}
		
		File tabletDirectory = new File(allTabletsDirectory, name);
		if (!tabletDirectory.isDirectory() || !tabletDirectory.canRead()) {
			throw new RuntimeException("Cannot read directory for tablet:  " + name);
		}

		File[] allFiles = tabletDirectory.listFiles();
		List<File> results = new ArrayList<File>();
		for (File f : allFiles) {
			if (f.isFile() && f.canRead()) {
				results.add(f);
			}
		}
		return results.iterator();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tablet other = (Tablet) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}

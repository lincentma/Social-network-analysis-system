/*
 * Copyright (C) IBM Corp. 2009.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.sf.xrime.utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Directory Manager for algorithms.
 * Many algorithms in X-Rime need several steps, like pagerank, which will create directories to store temporary data. 
 * TempDirMgr is used to manage directories in such algorithms.
 */
public class TempDirMgr {
	/**
	 * prefix for temporary directory.
	 */
	private String prefix="/xrime-temp-";
	
	/**
	 * used directories.
	 */
	protected Set<Path> dirs;
	
	/**
	 * fileSystem to check directory
	 */
	protected FileSystem fileSystem=null;
	
	/**
	 * Constructor.
	 */
	public TempDirMgr() {
		this.dirs=new HashSet<Path>();
	}
	
	/**
	 * Constructor.
	 * @param conf Configuration used to create FileSystem.
	 */
	public TempDirMgr(Configuration conf) {
		this.dirs=new HashSet<Path>();
		try {
			fileSystem=FileSystem.get(conf);
		} catch (IOException e) {
			fileSystem=null;
		}
	}
	
	public synchronized Set<Path> getDirs() {
		return dirs;
	}

	public synchronized void addPath(Path path) {
		dirs.add(path);
	}

	public synchronized void removePath(Path path) {
		dirs.remove(path);
	}
	
	/**
	 * Delete all existing directories this object managed and remove corresponding
	 * records in this object. For non-existing ones, leave their records untouched.
	 * <p>
	 * Non-existing paths recorded by this mgr might been renamed by clients.
	 * @throws IOException
	 */
	public synchronized void deleteAll() {
		if(fileSystem!=null) {
			for(Path f : dirs) {
			  try {
			    if (fileSystem.exists(f)){
			      // Delete existing ones, both from file system and the records of this mgr.
			      fileSystem.delete(f, true);
			    }
				} catch (IOException e) {
					// we can do nothing?
					e.printStackTrace();
				}
			}
			// Clear the records.
      dirs.clear();
		}
	}
	
	public synchronized FileSystem getFileSystem() {
		return fileSystem;
	}

	public synchronized void setFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	public synchronized String getPrefix() {
		return prefix;
	}

	public synchronized void setPrefix(String prefix) {
		this.prefix = prefix;
	}
	
	/**
	 * Get a new temporary directory.
	 * @return directory.
	 * @throws IOException 
	 */
	public synchronized Path getTempDir() throws IOException {
		Path tempDir =
		      new Path(prefix+Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
		
		if(fileSystem!=null) {
			while (dirs.contains(tempDir) || fileSystem.exists(tempDir)) {
				tempDir = new Path(prefix+Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
			}
		} else {
			while (dirs.contains(tempDir)) {
				tempDir = new Path(prefix+Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
			}
		}

		dirs.add(tempDir);

		return tempDir;
	}
}

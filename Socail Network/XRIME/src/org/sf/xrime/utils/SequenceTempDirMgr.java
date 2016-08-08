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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Directory Manager for algorithms.
 * Directory name will be in sequence.
 */
public class SequenceTempDirMgr extends TempDirMgr {
	private int seqNum=1;
	private String prefix="tmp";

	public SequenceTempDirMgr() {
		super();
	}

	public SequenceTempDirMgr(int seqNum) {
		super();
		this.seqNum=seqNum;
	}

	public SequenceTempDirMgr(String prefix) {
		super();
		this.prefix=prefix;
	}
	
	public SequenceTempDirMgr(Path prefix) {
		super();
		this.prefix=prefix.toString();
	}

	public SequenceTempDirMgr(String prefix, int seqNum) {
		super();
		this.seqNum=seqNum;
		this.prefix=prefix;
	}

	public SequenceTempDirMgr(String prefix, Configuration conf) {
		super(conf);
		this.prefix=prefix;
	}
	
	public SequenceTempDirMgr(Path prefix, Configuration conf) {
		super(conf);
		this.prefix=prefix.toString();
	}

	public SequenceTempDirMgr(String prefix, int seqNum, Configuration conf) {
		super(conf);
		this.seqNum=seqNum;
		this.prefix=prefix;
	}

	public synchronized int getSeqNum() {
		return seqNum;
	}

	public synchronized void setSeqNum(int seqNum) {
		this.seqNum = seqNum;
	}

	public synchronized String getPrefix() {
		return prefix;
	}

	public synchronized void setPrefix(String prefix) {
		this.prefix = prefix;
	}	

	public synchronized Path getTempDir() throws IOException {
		Path tempDir =
		      new Path(prefix+Integer.toString(seqNum++));
		
		if(fileSystem!=null) {
			while (dirs.contains(tempDir) || fileSystem.exists(tempDir)) {
				tempDir = new Path(prefix+Integer.toString(seqNum++));
			}
		} else {
			while (dirs.contains(tempDir)) {
				tempDir = new Path(prefix+Integer.toString(seqNum++));
			}
		}

		dirs.add(tempDir);

		return tempDir;
	}
}

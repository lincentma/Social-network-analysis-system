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

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.sf.xrime.ProcessorExecutionException;


/**
 * This class is used to retrieve those values which are output on the console
 * to summarize the execution of a map-reduce job.
 */
public class MRConsoleReader {
	/**
	 * Utility function.
	 * 
	 * @param job
	 * @param group_name
	 * @param counter_name
	 * @return the counter value, -1 if not found.
	 * @throws ProcessorExecutionException
	 */
	public static long getRecordNum(RunningJob job, String group_name,
			String counter_name) throws ProcessorExecutionException {
		try {
			long result = -1;
			for (Group group : job.getCounters()) {
				if (group.getDisplayName().compareTo(group_name) == 0) {
					for (Counter counter : group) {
						if (counter.getDisplayName().compareTo(counter_name) == 0) {
							result = counter.getCounter();
							break;
						}
					}
					break;
				}
			}
			return result;
		} catch (IOException e) {
			throw new ProcessorExecutionException(e);
		}
	}

	/**
	 * Utility method. Get "Map-Reduce Framework"->"Reduce output records".
	 * 
	 * @param job
	 * @return
	 * @throws IOException
	 * @throws ProcessorExecutionException
	 */
	public static long getReduceOutputRecordNum(RunningJob job)
			throws ProcessorExecutionException {
		return getRecordNum(job, "Map-Reduce Framework",
				"Reduce output records");
	}

	/**
	 * Utility function.
	 * 
	 * @param job
	 * @return
	 * @throws ProcessorExecutionException
	 */
	public static long getMapInputRecordNum(RunningJob job)
			throws ProcessorExecutionException {
		return getRecordNum(job, "Map-Reduce Framework", "Map input records");
	}

	/**
	 * Utility function.
	 * 
	 * @param job
	 * @return
	 * @throws ProcessorExecutionException
	 */
	public static long getMapOutputRecordNum(RunningJob job)
			throws ProcessorExecutionException {
		return getRecordNum(job, "Map-Reduce Framework", "Map output records");
	}
}

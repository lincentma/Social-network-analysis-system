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
package org.sf.xrime.algorithms.utils;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.sf.xrime.algorithms.GraphAlgorithmContext;


public class GraphAlgorithmMapReduceBase extends MapReduceBase {
  /** The context of this algorithm's execution. */
	protected GraphAlgorithmContext context;
	
	/** Default implementation that does nothing. */
	public void close() throws IOException {
	}
	
	/**
	 * Load graph algorithm context from job configuration.
	 */
	public void configure(JobConf job) {
	  super.configure(job);
		context=new GraphAlgorithmContext(job, true);
	}
}

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
package org.sf.xrime.algorithms.transform.edge;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.GraphAlgorithmContext;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.AdjVertex;


public class EdgeMapper extends MapReduceBase implements Mapper<Text, AdjVertex, Text, AdjVertex> {
	Graph destination;
	
	@Override
	public void map(Text key, AdjVertex value,
			OutputCollector<Text, AdjVertex> output, Reporter reporter)
			throws IOException {
		if(! destination.isAllowSelfloop() ) {
			value.removeLoop();
		}

		if(! destination.isAllowMultipleEdges() ) {
			value.removeMultipleEdge();
		}
		
		output.collect(key, value);
	}
	
	public void configure(JobConf job) {
		destination=new GraphAlgorithmContext(job,true).getDestinationOrDefault();
	}
}

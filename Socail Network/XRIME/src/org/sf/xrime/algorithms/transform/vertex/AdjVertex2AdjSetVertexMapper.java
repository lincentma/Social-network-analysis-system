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
package org.sf.xrime.algorithms.transform.vertex;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.AdjVertex;
import org.sf.xrime.utils.KeyValuePair;


public class AdjVertex2AdjSetVertexMapper extends MapReduceBase 
    implements Mapper<Text, AdjVertex, Text, ObjectWritable> {
    Text outputKey=new Text();
    AdjSetVertex outputVertex=new AdjSetVertex();
    ObjectWritable outputValue=new ObjectWritable();

	private AdjVertex2AdjSetVertexTransformer.EdgeFilter filter;
    
	@Override
	public void map(Text key, AdjVertex value,
			OutputCollector<Text, ObjectWritable> output, Reporter reporter)
			throws IOException {
		outputVertex.setId(value.getId());
		
		// emit vertex
		outputKey.set(value.getId());
		outputValue.set(outputVertex);
		output.collect(outputKey, outputValue);
		
		// emit edge
		Iterator<AbstractEdge>  iter=value.getIncidentElements();
		while(iter.hasNext()) {
			Edge edge=(Edge) iter.next();
			
			outputValue.set(edge);
			
			List<KeyValuePair<String, Edge>> pairs=filter.mapFilter(outputVertex, edge);
			
			for(KeyValuePair<String, Edge> pair : pairs) {
				outputKey.set(pair.getKey());
				outputValue.set(pair.getValue());
				output.collect(outputKey, outputValue);
			}
		}
	}

	public void configure(JobConf job) {
		super.configure(job);
		
		Class<? extends AdjVertex2AdjSetVertexTransformer.EdgeFilter> 
		    labelAdderClass=job.getClass(AdjVertex2AdjSetVertexTransformer.edgeFilterKey, 
				                         AdjVertex2AdjSetVertexTransformer.EverythingEdgeFilter.class, 
				                         AdjVertex2AdjSetVertexTransformer.EdgeFilter.class);
		try {
			filter=labelAdderClass.newInstance();
		} catch (InstantiationException e) {
			filter=new AdjVertex2AdjSetVertexTransformer.EverythingEdgeFilter();
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			filter=new AdjVertex2AdjSetVertexTransformer.EverythingEdgeFilter();
			e.printStackTrace();
		}
	}
}

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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.edge.EdgeUtils;
import org.sf.xrime.model.vertex.AdjSetVertex;


public class AdjVertex2AdjSetVertexReducer extends MapReduceBase implements
    Reducer<Text, ObjectWritable, Text, AdjSetVertex> {
    Text outputKey=new Text();
	
	@Override
	public void reduce(Text key, Iterator<ObjectWritable> values,
			OutputCollector<Text, AdjSetVertex> output, Reporter reporter)
			throws IOException {
		ArrayList<Edge> edges=new ArrayList<Edge>();
		AdjSetVertex vertex=null;
		
		while (values.hasNext()) {
			ObjectWritable obj=values.next();
			
			if(obj.get() instanceof Edge) {
				edges.add((Edge) obj.get());
			}
			
			if(obj.get() instanceof AdjSetVertex) {
				vertex=(AdjSetVertex) obj.get();	
			}
		}
		
		if(vertex!=null) {
			for(Edge edge: edges) {
				vertex.addOpposite(EdgeUtils.getAdjVertexEdgeByEdge(vertex, edge));
			}
			
			outputKey.set(vertex.getId());
			output.collect(key, vertex);
		}
	}
}

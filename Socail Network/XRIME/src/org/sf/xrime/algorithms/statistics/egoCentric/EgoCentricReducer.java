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
package org.sf.xrime.algorithms.statistics.egoCentric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjSetVertex;


public class EgoCentricReducer extends MapReduceBase 
    implements Reducer<Text, AdjSetVertex, Text, Edge> {
	
	Text resultKey=new Text();
	Edge resultValue=new Edge();
	
	@Override
	public void reduce(Text key, Iterator<AdjSetVertex> values,
			OutputCollector<Text, Edge> output, Reporter reporter)
			throws IOException {
		List<AdjSetVertex> inputList=new ArrayList<AdjSetVertex>();
		
		while (values.hasNext()) {
			inputList.add( (AdjSetVertex) values.next().clone() );
		}
		
		for(int ii=0; ii<inputList.size(); ii++) {
			String middle=inputList.get(ii).getId();

			for(int jj=0; jj<inputList.size(); jj++) {
				Set<AdjVertexEdge> tos=inputList.get(jj).getOpposites();
				
				for(AdjVertexEdge to : tos) {
					if(to.getOpposite().compareTo(middle)==0) {
						resultValue.setFrom(middle);
						resultValue.setTo(key.toString());
						resultKey.set(inputList.get(jj).getId());
						output.collect(resultKey, resultValue);
					}
				}
			}
		}
	}
}

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
package org.sf.xrime.preprocessing.textadj;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjVertex;


/**
 * Mapper class used to transform data from adjacency lists in text file into AdjVertex objects.
 * It emits data in AdjVertex which will be merged in Reducer(SmthReducer).
 */
public class TextAdjMapper extends MapReduceBase implements Mapper<Object, Text, Text, AdjVertex> {
	private Text outputKey = new Text();

	private static final String separateRegStr="\\s";  // used in regular express
	private static Pattern separateReg= Pattern.compile(separateRegStr,Pattern.DOTALL);
	
	public void map(Object inputKey, Text inputValue, OutputCollector<Text, AdjVertex> collector,
			Reporter reporter) throws IOException {
		String inputline=inputValue.toString().trim();
		
		String[] items=separateReg.split(inputline);
		
		if(items.length==0 || items[0].length()==0) {
			return;
		}
		
		String source=items[0];
		AdjVertex adjVertex=new AdjVertex(source);
		
		try {
		    for(int ii=1;ii<items.length; ii++) {
				if(items[ii].length()==0) {
					continue;
				}
				
				Edge edge = new Edge();
				
				edge.setFrom(source);
				edge.setTo(items[ii]);
				adjVertex.addEdge(edge);
								
				outputKey.set(items[ii]);
				collector.collect(outputKey, new AdjVertex(items[ii]));  // to's AdjVertex
		    }

			outputKey.set(source);
			collector.collect(outputKey, adjVertex);  // from's AdjVertex
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

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
package org.sf.xrime.preprocessing.smth;

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
 * Mapper class used to transform raw data from newsmth.net into adjacency lists.
 * @author Cai Bin
 */
public class SmthMapper extends MapReduceBase implements Mapper<Object, Text, Text, AdjVertex> {
	private Text outputKey = new Text();
	private Edge edge = new Edge();

	private static final String friendsSeparate="+";  // used in regular express
	private static final String friendsSeparateRegStr="\\+";  // used in regular express
	private static Pattern friendsSeparateReg= Pattern.compile(friendsSeparateRegStr,Pattern.DOTALL);;
	private static final String posterSeparate=" ";
	
	public void map(Object inputKey, Text inputValue, OutputCollector<Text, AdjVertex> collector,
			Reporter reporter) throws IOException {
		String inputline=inputValue.toString();

		int friendsBeginPos=inputline.lastIndexOf(friendsSeparate);
		if(friendsBeginPos==-1) {
			return;
		}
		
		friendsBeginPos=inputline.lastIndexOf(posterSeparate, friendsBeginPos);
		String[] friends=friendsSeparateReg.split(inputline.substring(friendsBeginPos+1));
		int posterPos=inputline.lastIndexOf(posterSeparate, friendsBeginPos-1);
		String poster=inputline.substring(posterPos+1,friendsBeginPos);
		
		if(poster.length()==0) {
			return;
		}
		
		for(String replyer : friends) {
			if(replyer.length()==0) {
				continue;
			}

			outputKey.set(replyer);
			
			edge.setFrom(replyer);
			edge.setTo(poster);
			
			try {
				AdjVertex adjVertex=new AdjVertex(replyer);
				adjVertex.addEdge(edge);
				
				// why using AdjVertex not Edge is to make sure that 
				// vertex which outdegree is zero could be emitted. 
				collector.collect(outputKey, adjVertex);  // from's AdjVertex				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		collector.collect(new Text(poster), new AdjVertex(poster));  // to's AdjVertex
	}
}

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
package org.sf.xrime.example.pagerank;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.pagerank.normal.AddPageRankLabelTransformer;
import org.sf.xrime.algorithms.pagerank.normal.PageRankAlgorithm;
import org.sf.xrime.algorithms.statistics.VertexEdgeCounter;
import org.sf.xrime.algorithms.statistics.VertexEdgeCounter.GeneralCounterFilter;
import org.sf.xrime.algorithms.transform.vertex.AdjVertex2AdjSetVertexTransformer;
import org.sf.xrime.algorithms.transform.vertex.AdjVertex2AdjSetVertexTransformer.NPlusEdgeFilter;
import org.sf.xrime.model.Graph;
import org.sf.xrime.preprocessing.smth.SmthTransformer;
import org.sf.xrime.preprocessing.textadj.TextAdjTransformer;

/**
 * This example shows how to compute PageRank score.
 * The graph in data directory is constructed using http://en.wikipedia.org/wiki/File:Linkstruct2.svg .
 */
public class PageRankExample {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//{{ process the arguments.
		String[] remainingArgs = new GenericOptionsParser(new Configuration(), args).getRemainingArgs();
		List<String> other_args = new ArrayList<String>();
		int mapperNum=5;    //default 1
		int reducerNum=5;   //default 1
		boolean isSmth=false;

		for(int i=0; i < remainingArgs.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					mapperNum=Integer.parseInt(args[++i]);
				} else if ("-r".equals(args[i])) {
					reducerNum=Integer.parseInt(args[++i]);
				} else if ("-s".equals(args[i])) {
					isSmth=true;
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return;
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						args[i-1]);
				return;
			}
		}
		
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					other_args.size() + " instead of 2.");
			return;
		}
		//}} process the arguments.	
		
		try {
			//{{ read the graph and convert it to internal data format.
			Path internalRawData=new Path(other_args.get(0)+".data");
			Path internalAdjSetData=new Path(other_args.get(0)+".adjSet");
			Path internalLabelData=new Path(other_args.get(0)+".label");
			Path counterPath=new Path(other_args.get(0)+".count");
			
			if(isSmth) {
				SmthTransformer transformer = new SmthTransformer();
				transformer.setSrcPath(new Path(other_args.get(0)));
				transformer.setDestPath(internalRawData);
				transformer.setMapperNum(mapperNum);
				transformer.setReducerNum(reducerNum);
				transformer.execute();				
			} else {
				TextAdjTransformer transformer = new TextAdjTransformer();
				transformer.setSrcPath(new Path(other_args.get(0)));
				transformer.setDestPath(internalRawData);
				transformer.setMapperNum(mapperNum);
				transformer.setReducerNum(reducerNum);
				transformer.execute();
			}
			//}} read the graph and convert it to internal data format.	

			//{{ convert data from AdjVertex to AdjSetVertex
			AdjVertex2AdjSetVertexTransformer toAdjSetVertexTransformer=new AdjVertex2AdjSetVertexTransformer();
			toAdjSetVertexTransformer.setSrcPath(internalRawData);
			toAdjSetVertexTransformer.setDestPath(internalAdjSetData);
			toAdjSetVertexTransformer.setMapperNum(mapperNum);
			toAdjSetVertexTransformer.setReducerNum(reducerNum);
			toAdjSetVertexTransformer.setEdgeFilter(NPlusEdgeFilter.class);
			toAdjSetVertexTransformer.execute();
			//}} convert data from AdjVertex to AdjSetVertex
			
			//{{ compute the vertex number, which used to initial pagerank label
			VertexEdgeCounter counter = new VertexEdgeCounter();

			Graph src = new Graph(Graph.defaultGraph());
			src.setPath(internalRawData);
			Graph dest = new Graph(Graph.defaultGraph());
			dest.setPath(counterPath);

			counter.setSource(src);
			counter.setDestination(dest);
			counter.execute();
			long vertexCount=counter.getCounter(GeneralCounterFilter.vertexCounterKey); 
			System.out.println("=====================================");
			System.out.println("There are "+vertexCount+" vertex(s) in the input graph!");
			System.out.println("=====================================");
			//}} compute the vertex number, which used to initial pagerank label
			
			//{{ add label: PageRankLabel
			AddPageRankLabelTransformer addLabelTransformer=new AddPageRankLabelTransformer();
			addLabelTransformer.setInitValue(1/(double)vertexCount);
			addLabelTransformer.setSrcPath(internalAdjSetData);
			addLabelTransformer.setDestPath(internalLabelData);
			addLabelTransformer.setMapperNum(mapperNum);
			addLabelTransformer.setReducerNum(reducerNum);
			addLabelTransformer.execute();
			//}} add label: PageRankLabel
			
			//{{ now, we can execute PageRankAlgorithm 
		    PageRankAlgorithm pr=new PageRankAlgorithm();
		    pr.setSrcPath(internalLabelData);
		    pr.setDestPath(new Path(other_args.get(1)));
		    pr.setMapperNum(mapperNum);
		    pr.setReducerNum(reducerNum);
		    pr.setMaxStep(150);
		    pr.setDampingFactor(1.0);
		    pr.setStopThreshold(0.02/vertexCount);  //only a suggestion
		    pr.execute();
			//}} now, we can execute PageRankAlgorithm 			
		} catch (ProcessorExecutionException e) {
			e.printStackTrace();
		}
	}
}

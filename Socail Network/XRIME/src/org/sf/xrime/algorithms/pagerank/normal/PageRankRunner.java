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
package org.sf.xrime.algorithms.pagerank.normal;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
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
public class PageRankRunner extends GraphAlgorithm{
  private boolean isSmth = false;
  private String srcPath = null;
  private String destPath = null;
  
	@Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
	  //{{ process the arguments.
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < params.length; ++i) {
			try {
				if ("-s".equals(params[i])) {
					isSmth=true;
				} else {
					other_args.add(params[i]);
				}
			} catch (NumberFormatException except) {
				throw new ProcessorExecutionException("Integer expected instead of " + params[i]);
			} catch (ArrayIndexOutOfBoundsException except) {
				throw new ProcessorExecutionException("Required parameter missing from " + params[i-1]);
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			throw new ProcessorExecutionException("Wrong number of parameters: " +
			    other_args.size() + " instead of 2.");
		}
		srcPath = other_args.get(0);
		destPath = other_args.get(1);
		//}} process the arguments.	
  }

  /**
	 * @param args
	 */
	public static void main(String[] args) {		
	  try {		  
		int res = ToolRunner.run(new PageRankRunner(), args);
		System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }	  
	}

  @Override
  public void execute() throws ProcessorExecutionException {
    // {{ read the graph and convert it to internal data format.
    Path internalRawData = new Path(srcPath + ".data");
    Path internalAdjSetData = new Path(srcPath + ".adjSet");
    Path internalLabelData = new Path(srcPath + ".label");
    Path counterPath = new Path(srcPath + ".count");

    if (isSmth) {
      SmthTransformer transformer = new SmthTransformer();
      transformer.setConf(context);
      transformer.setSrcPath(new Path(srcPath));
      transformer.setDestPath(internalRawData);
      transformer.setMapperNum(getMapperNum());
      transformer.setReducerNum(getReducerNum());
      transformer.execute();
    } else {
      TextAdjTransformer transformer = new TextAdjTransformer();
      transformer.setConf(context);
      transformer.setSrcPath(new Path(srcPath));
      transformer.setDestPath(internalRawData);
      transformer.setMapperNum(getMapperNum());
      transformer.setReducerNum(getReducerNum());
      transformer.execute();
    }
    // }} read the graph and convert it to internal data format.

    // {{ convert data from AdjVertex to AdjSetVertex
    AdjVertex2AdjSetVertexTransformer toAdjSetVertexTransformer = new AdjVertex2AdjSetVertexTransformer();
    toAdjSetVertexTransformer.setConf(context);
    toAdjSetVertexTransformer.setSrcPath(internalRawData);
    toAdjSetVertexTransformer.setDestPath(internalAdjSetData);
    toAdjSetVertexTransformer.setMapperNum(getMapperNum());
    toAdjSetVertexTransformer.setReducerNum(getReducerNum());
    toAdjSetVertexTransformer.setEdgeFilter(NPlusEdgeFilter.class);
    toAdjSetVertexTransformer.execute();
    // }} convert data from AdjVertex to AdjSetVertex

    // {{ compute the vertex number, which used to initial pagerank label
    VertexEdgeCounter counter = new VertexEdgeCounter();
    counter.setConf(context);
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(internalRawData);
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(counterPath);
    counter.setSource(src);
    counter.setDestination(dest);
    counter.execute();
    long vertexCount = counter
        .getCounter(GeneralCounterFilter.vertexCounterKey);
    System.out.println("=====================================");
    System.out.println("There are " + vertexCount
        + " vertex(s) in the input graph!");
    System.out.println("=====================================");
    // }} compute the vertex number, which used to initial pagerank label

    // {{ add label: PageRankLabel
    AddPageRankLabelTransformer addLabelTransformer = new AddPageRankLabelTransformer();
    addLabelTransformer.setConf(context);
    addLabelTransformer.setInitValue(1 / (double) vertexCount);
    addLabelTransformer.setSrcPath(internalAdjSetData);
    addLabelTransformer.setDestPath(internalLabelData);
    addLabelTransformer.setMapperNum(getMapperNum());
    addLabelTransformer.setReducerNum(getReducerNum());
    addLabelTransformer.execute();
    // }} add label: PageRankLabel

    // {{ now, we can execute PageRankAlgorithm
    PageRankAlgorithm pr = new PageRankAlgorithm();
    pr.setConf(context);
    pr.setSrcPath(internalLabelData);
    pr.setDestPath(new Path(destPath));
    pr.setMapperNum(getMapperNum());
    pr.setReducerNum(getReducerNum());
    pr.setMaxStep(150);
    pr.setDampingFactor(1.0);
    pr.setStopThreshold(0.02 / vertexCount); // only a suggestion
    pr.execute();
    // }} now, we can execute PageRankAlgorithm
  }
}

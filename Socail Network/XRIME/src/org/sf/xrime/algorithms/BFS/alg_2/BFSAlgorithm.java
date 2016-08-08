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
package org.sf.xrime.algorithms.BFS.alg_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.BFS.ExtractPathLength;
import org.sf.xrime.model.Graph;
import org.sf.xrime.utils.MRConsoleReader;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * BFS, find a shortest path from a specified starting vertex for each vertex reachable from the specified vertex.
 * @see org.sf.xrime.algorithms.setBFS.SetBFSAlgorithm
 * @author weixue@cn.ibm.com
 */
public class BFSAlgorithm extends GraphAlgorithm {
  

  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    List<String> other_args = new ArrayList<String>();
    String init_vertex = null;
    for(int i=0; i < params.length; i++) {
      if ("-init".equals(params[i])) {
        // id of the starting vertex of BFS.
        init_vertex = params[++i];
      } else {
        other_args.add(params[i]);
      }
    }
    
    if(init_vertex == null)
      throw new ProcessorExecutionException("You need to specify the starting vertex of BFS.");
        // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters left: " +
                         other_args.size() + " instead of 2.");
    }

    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(new Path(other_args.get(0)));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(other_args.get(1)));
    setSource(src);
    setDestination(dest);
    
    setParameter(ConstantLabels.INIT_VERTEX, init_vertex);
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      if(getSource().getPaths()==null||getSource().getPaths().size()==0||
          getDestination().getPaths()==null||getDestination().getPaths().size()==0){
        throw new ProcessorExecutionException("No input and/or output paths specified.");
      }
      SequenceTempDirMgr dirMgr = new SequenceTempDirMgr(context.getDestination().getPath().toString() + "/", context);
      dirMgr.setSeqNum(0);

      BFSStep bfsStep = new BFSStep();
      bfsStep.setConf(context);
      bfsStep.getContext().setSource(getSource());
      Graph dest = new Graph(getSource());
      dest.setPath(dirMgr.getTempDir());
      bfsStep.getContext().setDestination(dest);
      bfsStep.setMapperNum(getMapperNum());
      bfsStep.setReducerNum(getReducerNum());

      while (true) {
        System.out.println("++++++>" + dirMgr.getSeqNum() +": BFSStep.");
        bfsStep.execute();
        
        RunningJob step_result = bfsStep.getFinalStatus();
        long frontier_size = MRConsoleReader.getRecordNum(step_result, ConstantLabels.FRONTIER_SIZE, 
            ConstantLabels.FRONTIER_SIZE);
        if(frontier_size <= 0)
          break; // Propagation finished.        
        bfsStep.setSource(bfsStep.getContext().getDestination());
        dest = new Graph(getSource());
        dest.setPath(dirMgr.getTempDir());
        bfsStep.getContext().setDestination(dest);
      }
      
      ExtractPathLength extract_pl = new ExtractPathLength();
      extract_pl.setConf(context);
      extract_pl.setSource(bfsStep.getDestination());
      dest = new Graph(getSource());
      dest.setPath(dirMgr.getTempDir());
      extract_pl.setDestination(dest);
      extract_pl.setMapperNum(getMapperNum());
      extract_pl.setReducerNum(getReducerNum());
      System.out.println("++++++>" + dirMgr.getSeqNum()+": ExtractPathLength.");
      extract_pl.execute();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

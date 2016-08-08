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
package org.sf.xrime.algorithms.partitions.connected.weakly.alg_1;

import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.transform.vertex.AdjVertex2VertexSetTransformer;
import org.sf.xrime.model.Graph;
import org.sf.xrime.utils.SequenceTempDirMgr;

/**
 * All in one algorithm to calculate weekly connected components of a graph, which is 
 * based on set operations.
 * @author xue
 */
public class SetBasedAlgorithm extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public SetBasedAlgorithm(){
    super();
  }
  
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    // Make sure there are exactly 2 parameters left.
    if (params.length != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters: " +
                         params.length + " instead of 2.");
    }

    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(new Path(params[0]));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(params[1]));
    
    setSource(src);
    setDestination(dest);
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      if(getSource().getPaths()==null||getSource().getPaths().size()==0||
          getDestination().getPaths()==null||getDestination().getPaths().size()==0){
        throw new ProcessorExecutionException("No input and/or output paths specified.");
      }
      
      // The prefix used by temp directories which store intermediate results of each steps.
      String temp_dir_prefix = getDestination().getPath().getParent().toString()+"/wcc_alg1_"+
                          getDestination().getPath().getName()+"_";
      
      // Create the temporary directory manager.
      SequenceTempDirMgr dirMgr = new SequenceTempDirMgr(temp_dir_prefix, context);
      // Sequence number begins with zero.
      dirMgr.setSeqNum(0);
      Path tmpDir;
    
      // 1. Transform input from outgoing adjacency vertexes lists to VertexSet.
      System.out.println("~~~~~~~~~~>"+dirMgr.getSeqNum()+": Transform input to VertexSet");
      Transformer transformer = new AdjVertex2VertexSetTransformer();
      transformer.setConf(context);
      transformer.setSrcPath(getSource().getPath());
      // Generate temporary directory.
      tmpDir = dirMgr.getTempDir();
      // And use it as the destination directory.
      transformer.setDestPath(tmpDir);
      transformer.setMapperNum(getMapperNum());
      transformer.setReducerNum(getReducerNum());
      transformer.execute();
      
      long map_input_records_num = -1; 
      long map_output_records_num = -2;
      Graph src;
      Graph dest;
      Stack<Path> expanding_stack = new Stack<Path>();
      // 2. loop of MinorJoin + Join.
      do{
        // 2.1 MinorJoin.
        System.out.println("~~~~~~~~~~>"+dirMgr.getSeqNum()+": VertexSetMinorJoin");
        GraphAlgorithm minorjoin = new VertexSetMinorJoin();
        minorjoin.setConf(context);
        src = new Graph(Graph.defaultGraph());
        // Use the output directory of last step as the input directory of this step.
        src.setPath(tmpDir);
        dest = new Graph(Graph.defaultGraph());
        // Generate a new temporary directory.
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        minorjoin.setSource(src);
        minorjoin.setDestination(dest);
        minorjoin.setMapperNum(getMapperNum());
        minorjoin.setReducerNum(getReducerNum());
        minorjoin.execute();
        
        // Push the join result to the stack.
        expanding_stack.push(tmpDir);
        
        // 2.2 Join.
        System.out.println("~~~~~~~~~~>"+dirMgr.getSeqNum()+": VertexSetJoin");
        GraphAlgorithm join = new VertexSetJoin();
        join.setConf(context);
        src = new Graph(Graph.defaultGraph());
        src.setPath(tmpDir);
        dest = new Graph(Graph.defaultGraph());
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        join.setSource(src);
        join.setDestination(dest);
        join.setMapperNum(getMapperNum());
        join.setReducerNum(getReducerNum());
        join.execute();
        
        // 2.3 Check for convergence.
        RunningJob join_result = join.getFinalStatus();
        boolean found_map_inrec = false;
        boolean found_map_outrec = false;
        for(Group group : join_result.getCounters()){
          if(group.getDisplayName().compareTo("Map-Reduce Framework")==0){
            for(Counter counter : group){
              if(counter.getDisplayName().compareTo("Map input records")==0){
                found_map_inrec = true;
                map_input_records_num = counter.getCounter();
              }else if(counter.getDisplayName().compareTo("Map output records")==0){
                found_map_outrec = true;
                map_output_records_num = counter.getCounter();
              }
            }
          }
        }
        if(!found_map_inrec || !found_map_outrec) 
          throw new ProcessorExecutionException("Can't find the map input/output record number.");
        System.out.println("~~~~~~~~~~>input/output: "+map_input_records_num+" vs. "+map_output_records_num);
      }while(map_input_records_num!=map_output_records_num);
      
      // 3. Expand, i.e., propagate the labels.
      while(expanding_stack.size()>0){
        // 3.1 Expand
        System.out.println("~~~~~~~~~~>"+dirMgr.getSeqNum()+": VertexSetExpand");
        GraphAlgorithm expand = new VertexSetExpand();
        expand.setConf(context);
        src = new Graph(Graph.defaultGraph());
        src.addPath(expanding_stack.pop());
        src.addPath(tmpDir);
        dest = new Graph(Graph.defaultGraph());
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        expand.setSource(src);
        expand.setDestination(dest);
        expand.setMapperNum(getMapperNum());
        expand.setReducerNum(getReducerNum());
        expand.execute();
        
        // 3.2 MinorExpand
        System.out.println("~~~~~~~~~~>"+dirMgr.getSeqNum()+": VertexSetMinorExpand");
        GraphAlgorithm minorexpand = new VertexSetMinorExpand();
        minorexpand.setConf(context);
        src = new Graph(Graph.defaultGraph());
        src.setPath(tmpDir);
        dest = new Graph(Graph.defaultGraph());
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        minorexpand.setSource(src);
        minorexpand.setDestination(dest);
        minorexpand.setMapperNum(getMapperNum());
        minorexpand.setReducerNum(getReducerNum());
        minorexpand.execute();
      }
      
      Path expanding_final = tmpDir;
      
      // 4. Summarize sets.
      System.out.println("~~~~~~~~~~>"+dirMgr.getSeqNum()+": VertexSetSummarize");
      GraphAlgorithm summarize = new VertexSetSummarize();
      summarize.setConf(context);
      src = new Graph(Graph.defaultGraph());
      src.setPath(tmpDir);
      dest = new Graph(Graph.defaultGraph());
      tmpDir = dirMgr.getTempDir();
      dest.setPath(tmpDir);
      summarize.setSource(src);
      summarize.setDestination(dest);
      summarize.setMapperNum(getMapperNum());
      summarize.setReducerNum(getReducerNum());
      summarize.execute();
      
      // 5. Extract WCC as partitions.
      System.out.println("~~~~~~~~~~>"+dirMgr.getSeqNum()+": ExtractPartitions");
      GraphAlgorithm extract = new ExtractPartitions();
      extract.setConf(context);
      src = new Graph(Graph.defaultGraph());
      src.setPath(expanding_final);
      dest = new Graph(Graph.defaultGraph());
      dest.setPath(getDestination().getPath());
      extract.setSource(src);
      extract.setDestination(dest);
      extract.setMapperNum(getMapperNum());
      extract.setReducerNum(getReducerNum());
      extract.execute();
      
      // Delete all temporary directories.
      dirMgr.deleteAll();
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new SetBasedAlgorithm(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

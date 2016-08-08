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
package org.sf.xrime.algorithms.setBFS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * Reducer of SetBFS. The major part of this algorithm.
 * @author Cai Bin
 */
public class SetBFSReducer extends GraphAlgorithmMapReduceBase 
    implements Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex> {
  JobConf job=null;
  
  Text outputKey=new Text();

  List<String> preps=new ArrayList<String>();
  int distance;
  
  /**
   * Flag, whether there is an edge emitted.
   * The emitting means we need run another BFS step since new edges is visited. 
   */
  boolean changeFlag=false;  

  /**
   * Reducer for BFS algorithm.
   * The inputs of this function would be:
   * 1: unvisited vertex
   * Key is equal to vertex id means that this is a vertex. BFSLabel.status is 0. String flag is U.
    * 2: visited vertex
   * SetBFSLabel.status is 1. String flag is V.
   * 3: visited edge
   * Key is not equal to vertex id means that this is an edge. And the destination of this edge, if is not visited, would be visited in the next step.
   * 4: init vertex
   * SetBFSLabel.status is -1. String flag is I.
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
      OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
      throws IOException {
    LabeledAdjSetVertex inputBFSVertex=null;
    LabeledAdjSetVertex initBFSVertex=null;
    
    preps.clear();
    distance=Integer.MAX_VALUE;
        
    while (values.hasNext()) {
      LabeledAdjSetVertex vertex=new LabeledAdjSetVertex(values.next());
      
      SetBFSLabel label=(SetBFSLabel) vertex.getLabel(SetBFSLabel.setBFSLabelPathsKey);
      
      if(label==null) {
        label=new SetBFSLabel();
        vertex.setLabel(SetBFSLabel.setBFSLabelPathsKey, label);
      }
      
      if(label.getStatus()==-1) { // init vertex
        initBFSVertex=vertex;
        distance=0;
        continue;
      }
      
      if(vertex.getId().compareTo(key.toString())!=0) {
        preps.add(vertex.getId());  // edge
        if(label.getDistance()<distance) {
          distance=label.getDistance();
        }
        continue;
      }
      
      inputBFSVertex=vertex; // vertex
      
      if(label.getStatus()==1) {  // visited
        output.collect(key, inputBFSVertex);
        return;
      }
    }
    
    if(!preps.isEmpty()) {    
      if(((SetBFSLabel)inputBFSVertex.getLabel(SetBFSLabel.setBFSLabelPathsKey)).getStatus()==0) {
        visiteNode(inputBFSVertex, output);  // visit the vertex
      }
      
      return;
    }
    
    if( (initBFSVertex!=null) && (inputBFSVertex!=null) ) {  // process the init node
      visiteNode(inputBFSVertex, output);
      return;
    }
    
    
    output.collect(key, inputBFSVertex);  // no change vertex
  }
  
  /**
   * Visit a vertex in BFS algorithm
   * @param prep a possible BFS visiting path.
   * @param now vertex to be visited
   * @param output OutputCollector of reducer
   * @throws IOException OutputCollector may throw IOException.
   */
  private void visiteNode(LabeledAdjSetVertex now, OutputCollector<Text, LabeledAdjSetVertex> output) throws IOException {
    SetBFSLabel nowLabel  = (SetBFSLabel) now.getLabel(SetBFSLabel.setBFSLabelPathsKey);
    
    nowLabel.addPreps(preps);  // we only record one possible path
    nowLabel.setStatus(1);  // set flag
    nowLabel.setDistance(distance);
    outputKey.set(now.getId());
    output.collect(outputKey, now);
    
    Iterator<AbstractEdge> iter=(Iterator<AbstractEdge>) now.getIncidentElements();
    
    nowLabel.setDistance(distance+1);
    now=(LabeledAdjSetVertex) now.clone();
    now.setOpposites(new HashSet<AdjVertexEdge>());
    
    while(iter.hasNext()) {  // emit all edges, which source are the visited vertex.
      if(!changeFlag) {
        recordContinue();
      }
      
      outputKey.set( ((AdjVertexEdge)iter.next()).getOpposite() );
      output.collect(outputKey, now);
    }
  }

  /** 
   * Keep JobConf to create FileSystem.
   * @see org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    super.configure(job);
    this.job=job;
  }
  
  /**
   * Record an edge emits in file system. We create a directory in file system as a flag.
   * BFSStep will check the directory, if it is existed, we need a next step in BFS algorithm.
   * If no edge emitted, we have visited all reachable elements in the graph. The algorithm will end.
   * This is a communication mechanism in Map/Reduce to indicate a event.
   * @throws IOException indicate error in creating directory.
   */
  private void recordContinue() throws IOException {
    if(changeFlag) {  // we only need to create the directory once.
      return;
    }
    
    changeFlag=true;
    
    String continueFile=context.getParameter(SetBFSStep.continueFileKey);
      
    if(continueFile!=null) {  // create the directory.
      FileSystem client=FileSystem.get(job);
      client.mkdirs(new Path(continueFile));
    }
  }
}

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
package org.sf.xrime.algorithms.BFS.alg_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.BFS.BFSLabel;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.LabeledAdjVertex;


/**
 * Reducer of BFS. The major part of this algorithm.
 * @author Cai Bin
 */
public class BFSReducer extends GraphAlgorithmMapReduceBase 
    implements Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  JobConf job=null;
  
  Text outputKey=new Text();
  
  /**
   * Flag, whether there is an edge emitted.
   * <p>
   * The emitting means we need run another BFS step since new edges is visited. 
   */
  boolean changeFlag=false;  

  /**
   * Reducer for BFS algorithm.
   * <p>
   * The inputs of this function would be:
   * <p>
   * 1: unvisited vertex
   * <p>
   * Key is equal to vertex id means that this is a vertex. BFSLabel.status is 0. String flag is U.
   * <p>
   * Example: Key: A; Value: &lt;A, &lt;&lt;A, B&gt;, &lt;A, C&gt;, &lt;A, D&gt;, &lt;A, E&gt;&gt;, &lt;xrime.algorithm.BFS.label, &lt;V, &lt;A&gt;&gt;&gt;&gt;
   * <p>
   * 2: visited vertex
   * <p>
   * BFSLabel.status is 1. String flag is V.
   * <p>
   * Example: Key: B; Value: &lt;B, &lt;&lt;B, C&gt;, &lt;B, F&gt;&gt;, &lt;xrime.algorithm.BFS.label, &lt;U&gt;&gt;&gt;
   * <p>
   * 3: visited edge
   * <p>
   * Key is not equal to vertex id means that this is an edge. And the destination of this edge, if is not visited, would be visited in the next step.
   * <p>
   * Example: Key: D; Value: &lt;E, &lt;&gt;, &lt;xrime.algorithm.BFS.label, &lt;V, &lt;A, E&gt;&gt;&gt;&gt;
   * <p>
   * 4: init vertex
   * <p>
   * BFSLabel.status is -1. String flag is I.
   * <p>
   * Example: Key: A; Value: &lt;A, &lt;&gt;, &lt;xrime.algorithm.BFS.label, &lt;I&gt;&gt;&gt;
   * <p>
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void reduce(Text key, Iterator<LabeledAdjVertex> values,
      OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
      throws IOException {
    LabeledAdjVertex inputBFSVertex=null;
    LabeledAdjVertex initBFSVertex=null;
    LabeledAdjVertex visitedBFSVertex=null;
    
    while (values.hasNext()) {
      LabeledAdjVertex vertex=new LabeledAdjVertex(values.next());
      
      BFSLabel label=(BFSLabel) vertex.getLabel(BFSLabel.bfsLabelPathsKey);
      
      if(label==null) {
        label=new BFSLabel();
        vertex.setLabel(BFSLabel.bfsLabelPathsKey, label);
      }
      
      if(label.getStatus()==-1) { // init vertex
        initBFSVertex=vertex;
        continue;
      }
      
      if(vertex.getId().compareTo(key.toString())!=0) {
        visitedBFSVertex=vertex;  // edge
        continue;
      }
      
      inputBFSVertex=vertex; // vertex
      
      if(label.getStatus()==1) {  // visited
        output.collect(key, inputBFSVertex);
        return;
      }
    }
    
    if(visitedBFSVertex!=null) {    
      if(((BFSLabel)inputBFSVertex.getLabel(BFSLabel.bfsLabelPathsKey)).getStatus()==0) {
        visiteNode(visitedBFSVertex, inputBFSVertex, output);  // visit the vertex
      }
      
      return;
    }
    
    if( (initBFSVertex!=null) && (inputBFSVertex!=null) ) {  // process the init node
      visiteNode(initBFSVertex, inputBFSVertex, output);
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
  private void visiteNode(LabeledAdjVertex prep, LabeledAdjVertex now, OutputCollector<Text, LabeledAdjVertex> output) throws IOException {
    BFSLabel prepLabel = (BFSLabel) prep.getLabel(BFSLabel.bfsLabelPathsKey);
    BFSLabel nowLabel  = (BFSLabel) now.getLabel(BFSLabel.bfsLabelPathsKey);
    
    nowLabel.addPreps(prepLabel.getPreps());  // we only record one possible path
    nowLabel.addPrep(now.getId());
    nowLabel.setStatus(1);  // set flag
    outputKey.set(now.getId());
    output.collect(outputKey, now);
    
    Iterator<AbstractEdge> iter=(Iterator<AbstractEdge>) now.getIncidentElements();
    
    now=(LabeledAdjVertex) now.clone();
    now.setEdges(new ArrayList<Edge>());
    
    while(iter.hasNext()) {  // emit all edges, which source are the visited vertex.
      if(!changeFlag) {
        recordContinue();
      }
      
      outputKey.set( ((Edge)iter.next()).getTo() );
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
    
    String continueFile=context.getParameter(BFSStep.continueFileKey);
      
    if(continueFile!=null) {  // create the directory.
      FileSystem client=FileSystem.get(job);
      client.mkdirs(new Path(continueFile));
    }
  }
}

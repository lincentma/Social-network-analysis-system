/*
 * Copyright (C) liuchangyan@BUPT. 2009.
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
package org.sf.xrime.algorithms.layout.radialtree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.sf.xrime.algorithms.layout.ConstantLabels;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * Reducer of RadialTree Layout Algorithm to get the depth of the tree and the distance
 * from root vertex to every vertex.
 * @author liu chang yan
 */
public class RadialTreeVisitReducer extends GraphAlgorithmMapReduceBase 
    implements Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  
  JobConf job=null;
  
  Text outputKey=new Text();
  
  /**
   * Flag, whether there is an edge emitted.
   * The emitting means we need run another RadialTree step1 since new edges is visited. 
   */
  boolean changeFlag=false;  

  /**
   * Reducer for RadialTree algorithm.
   * The inputs of this function would be:
   * 1: unvisited vertex
   * Key is equal to vertex id means that this is a vertex. RadialTreeLabel.status is 0. String flag is U.
   * Example: Key: A; Value: &lt;A, &lt;&lt;A, B&gt;, &lt;A, C&gt;, &lt;A, D&gt;, &lt;A, E&gt;&gt;, &lt;xrime.algorithm.RadialTree.label, &lt;V, &lt;A&gt;&gt;&gt;&gt;
   * 2: visited vertex
   * RadialTreeLabel.status is 1. String flag is V.
   * Example: Key: B; Value: &lt;B, &lt;&lt;B, C&gt;, &lt;B, F&gt;&gt;, &lt;xrime.algorithm.RadialTree.label, &lt;U&gt;&gt;&gt;
   * 3: visited edge
   * Key is not equal to vertex id means that this is an edge. And the destination of this edge, if is not visited, would be visited in the next step.
   * Example: Key: D; Value: &lt;E, &lt;&gt;, &lt;xrime.algorithm.RadialTree.label, &lt;V, &lt;A, E&gt;&gt;&gt;&gt;
   * 4: init vertex
   * RadialTreeLabel.status is -1. String flag is I.
   * Example: Key: A; Value: &lt;A, &lt;&gt;, &lt;xrime.algorithm.RadialTree.label, &lt;I&gt;&gt;&gt;
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void reduce(Text key, Iterator<LabeledAdjVertex> values,
      OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
      throws IOException {
      LabeledAdjVertex inputRadialTreeVertex=null;
      LabeledAdjVertex initRadialTreeVertex=null;
      LabeledAdjVertex visitedRadialTreeVertex=null;
    
      while (values.hasNext()) {
        LabeledAdjVertex vertex=new LabeledAdjVertex(values.next());
      
        RadialTreeLabel label=(RadialTreeLabel) vertex.getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);
        if(label==null) {
        label=new RadialTreeLabel();
        vertex.setLabel(RadialTreeLabel.RadialTreeLabelPathsKey, label);              
        }
      
        if(label.getStatus()==-1) { // init vertex
          initRadialTreeVertex=vertex;                
          continue;
        }
      
        if(vertex.getId().compareTo(key.toString())!=0) {
          visitedRadialTreeVertex=vertex;  // edge
          continue;
        }
      
        inputRadialTreeVertex=vertex; // vertex
      
        if(label.getStatus()==1) {  // visited
          output.collect(key, inputRadialTreeVertex);
          return;
        }
      }
    
      if(visitedRadialTreeVertex!=null) {    
        if(((RadialTreeLabel)inputRadialTreeVertex.getLabel(RadialTreeLabel.RadialTreeLabelPathsKey)).getStatus()==0) {
          visiteNode(visitedRadialTreeVertex, inputRadialTreeVertex, output);  // visit the vertex
        }      
        return;
      }
    
      if( (initRadialTreeVertex!=null) && (inputRadialTreeVertex!=null) ) {  // process the init node
        visiteNode(initRadialTreeVertex, inputRadialTreeVertex, output);
        return;
      }  
      output.collect(key, inputRadialTreeVertex);
  }
  
  /**
   * Visit a vertex in RadialTree algorithm
   * @param prep a possible RadialTree visiting path.
   * @param now vertex to be visited
   * @param output OutputCollector of reducer
   * @throws IOException OutputCollector may throw IOException.
   */
  private void visiteNode(LabeledAdjVertex prep, LabeledAdjVertex now, OutputCollector<Text, LabeledAdjVertex> output) throws IOException {
    RadialTreeLabel prepLabel = (RadialTreeLabel) prep.getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);
    RadialTreeLabel nowLabel  = (RadialTreeLabel) now.getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);
    
    long distance = 0;
    List<String> dist;
    dist=new ArrayList<String>();
    distance = prepLabel.getPreps().size() / 2;
    // Get size of the display frame.
    int max_x = Integer.parseInt(context.getParameter(ConstantLabels.MAX_X_COORDINATE));
    int max_y = Integer.parseInt(context.getParameter(ConstantLabels.MAX_Y_COORDINATE));
        
    // Caculate vertex weight
    double scale = (max_x < max_y)?max_x:max_y;
    double diameter = (distance == 0) ? 0 :Math.sqrt(scale*scale) / distance;

    dist.add(diameter+"");
    nowLabel.addPreps(prepLabel.getPreps());  // we only record one possible path
    
    // Add the previous vertices and them weights
    for(int i = 1;i < nowLabel.getPreps().size(); i = i + 2){
      double number = Double.parseDouble(nowLabel.getPreps().get(i)); 
      nowLabel.getPreps().set(i,(number+diameter+""));
    }
    Text outputKey=new Text();
    // Deal with label
    nowLabel.addPrep(now.getId());
    nowLabel.setStatus(1);  // set flag
    nowLabel.setWeight(diameter);  
    nowLabel.setDistance(distance);
    nowLabel.addPreps(dist);
    nowLabel.setPre(prep.getId());
    nowLabel.setPredistance(prepLabel.getDistance());
    nowLabel.setNext(now.getEdges().size());

    outputKey.set(now.getId());    
    
    if(nowLabel.getNext() == 0){
      nowLabel.setEnd(1);
    }

    output.collect(outputKey, now);
    Iterator<AbstractEdge> iter=(Iterator<AbstractEdge>) now.getIncidentElements();
  
    now=(LabeledAdjVertex) now.clone();
    now.setEdges(new ArrayList<Edge>());
    
    // emit all edges, which source are the visited vertex.
    while(iter.hasNext()) {  
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
   * RadialTreeStep will check the directory, if it is existed, we need a next step in RadialTree algorithm.
   * If no edge emitted, we have visited all reachable elements in the graph. The algorithm will end.
   * This is a communication mechanism in Map/Reduce to indicate a event.
   * @throws IOException indicate error in creating directory.
   */
  private void recordContinue() throws IOException {    
    if(changeFlag) {  // we only need to create the directory once.
      return;
    }
    
    changeFlag=true;
    
    String continueFile=context.getParameter(RadialTreeStep1.continueFileKey);
    
    if(continueFile!=null) {  // create the directory.
      FileSystem client=FileSystem.get(job);
      client.mkdirs(new Path(continueFile));
      client.close();
    }
  }
}

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
import org.sf.xrime.model.vertex.LabeledAdjVertex;


/**
 * Reducer of RadialTree Layout Algorithm to caculate the every subtree weight. Caaulate 
 * subtree weight by adding its weight and other subtrees' weight rooted by its successive
 *  vertices and emit the result to its previous vertex.
 * @author liu chang yan
 */
public class RadialTreeWeightReducer extends GraphAlgorithmMapReduceBase 
  implements Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  
  JobConf job=null;
  Text outputKey=new Text();
  boolean changeFlag=false;  
  
  public void reduce(Text key, Iterator<LabeledAdjVertex> values,
    OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
    throws IOException {    
    
    LabeledAdjVertex initRadialTreeVertex=null;
    RadialTreeLabel initLabel = new RadialTreeLabel();
    long now_distance = Long.parseLong((context.getParameter(ConstantLabels.NUM_OF_VERTEXES)));
    // Judge whether do the next step
    if(now_distance > 1) {
      if(!changeFlag) {
        recordContinue();
      }    
    }      
    // Using record the vertex weight
    double temp = 0;
      List<String> succ;
      succ = new ArrayList<String>();
    while(values.hasNext()) {
      LabeledAdjVertex vertex=new LabeledAdjVertex(values.next());
      RadialTreeLabel label=(RadialTreeLabel) vertex.getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);
      // Adding successive vertices and them weight
      if(label.getDistance() == now_distance && vertex.getId().compareTo(key.toString()) != 0){
        temp += label.getWeight();
        succ.add(vertex.getId());
        succ.add(label.getWeight()+"");
      }        
      // Accumulate weight
      if(vertex.getId().compareTo(key.toString()) == 0){
        initRadialTreeVertex = vertex;
        initLabel=label;
        temp += label.getWeight();
      }        
    }
    // Deal with label
    initLabel.setWeight(temp);      
    initLabel.setEnd(1);
    initLabel.addSucc(succ);
    output.collect(key, initRadialTreeVertex);  
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
    
    String continueFile=context.getParameter(RadialTreeStep2.continueFileKeyWeight);
      
    if(continueFile!=null) {  // create the directory.
      FileSystem client=FileSystem.get(job);
      client.mkdirs(new Path(continueFile));
      client.close();
    }
  }
}





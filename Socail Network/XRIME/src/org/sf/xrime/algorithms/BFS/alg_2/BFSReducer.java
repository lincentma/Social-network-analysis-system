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
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.BFS.BFSLabel;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;


/**
 * Reducer of BFS. Identify the frontier. 
 * @author weixue@cn.ibm.com
 */
public class BFSReducer extends GraphAlgorithmMapReduceBase 
    implements Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {

  @Override
  public void reduce(Text key, Iterator<LabeledAdjVertex> values,
      OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
      throws IOException {
    // The vertex of key.
    LabeledAdjVertex inputBFSVertex=null;
    // Notifier.
    LabeledAdjVertex visitedBFSVertex=null;
    
    while (values.hasNext()) {
      LabeledAdjVertex vertex = new LabeledAdjVertex(values.next());
      
      BFSLabel label=(BFSLabel) vertex.getLabel(BFSLabel.bfsLabelPathsKey);
      
      if(label == null) {
        // Just in case BFSLabelTransformer is not invoked?
        label = new BFSLabel();
        vertex.setLabel(BFSLabel.bfsLabelPathsKey, label);
      }
      
      if(vertex.getId().compareTo(key.toString())!=0) {
        if(visitedBFSVertex == null)
          visitedBFSVertex = vertex;  // Only take one path from the starting vertex.
        continue;
      }
      
      inputBFSVertex = vertex; // The vertex, not notifiers.
      
      if(label.getStatus() == 1) {  // 1. The vertex has been visited before.
        output.collect(key, inputBFSVertex);
        return;
      }
    }
    
    if(visitedBFSVertex != null) {    
      if(inputBFSVertex == null) System.out.println("" + key + " none inputBFSVertex found.");
      if(((BFSLabel)inputBFSVertex.getLabel(BFSLabel.bfsLabelPathsKey)).getStatus()==0) { // Necessary?
        visitNode(visitedBFSVertex, inputBFSVertex, output);  // 2. Advance the frontier.
        // Increase number.
        reporter.incrCounter(ConstantLabels.FRONTIER_SIZE, ConstantLabels.FRONTIER_SIZE, 1);
      }
      return;
    }
    
    output.collect(key, inputBFSVertex);  // 3. Non-visited vertex which is not changed in this round of iteration.
  }
  
  /**
   * Visit a vertex in BFS algorithm
   * @param prep a possible BFS visiting path.
   * @param now vertex to be visited
   * @param output collector
   */
  private void visitNode(LabeledAdjVertex prep, LabeledAdjVertex now, OutputCollector<Text, LabeledAdjVertex> output) throws IOException {
    BFSLabel prepLabel = (BFSLabel) prep.getLabel(BFSLabel.bfsLabelPathsKey);
    BFSLabel nowLabel  = (BFSLabel) now.getLabel(BFSLabel.bfsLabelPathsKey);
    
    // Add all precendents.
    nowLabel.addPreps(prepLabel.getPreps());  
    // Mark it as the frontier.
    now.setStringLabel(ConstantLabels.IS_FRONTIER, "true");
    // Won't change status here, this is left to mapper of next round of iteration.
    output.collect(new Text(now.getId()), now);
  }
}

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
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.BFS.BFSLabel;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.LabeledAdjVertex;


/**
 * Mapper of BFS. In this algorithm, we choose to "advance the known frontier" in Mapper. 
 * @author weixue@cn.ibm.com
 */
public class BFSMapper extends GraphAlgorithmMapReduceBase implements
  Mapper<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  /**
   * map method of BFSMapper. Emit info to adjacent vertexes if is the init vertex or in
   * the current frontier. 
   */
  @Override
  public void map(Text key, LabeledAdjVertex value,
      OutputCollector<Text, LabeledAdjVertex> collector, Reporter reporter)
      throws IOException {
    // Vertex id.
    String id = value.getId();
    // Init vertex id.
    String init_vertex = context.get(ConstantLabels.INIT_VERTEX, "");
    // Whether is this vertex in the current frontier.
    boolean is_frontier = value.getLabel(ConstantLabels.IS_FRONTIER)!=null;
    // Label of this vertex.
    BFSLabel label = (BFSLabel) value.getLabel(BFSLabel.bfsLabelPathsKey);
    // Whether we should emit info to neighbors.
    if(is_frontier || (label.getStatus() == 0 && id.equals(init_vertex))){
      // If this vertex is in current frontier, or this is the first round of iteration and this vertex is the
      // starting vertex.
      
      // Mark the vertex as visited.
      label.setStatus(1);
      // Add one more precedent vertex, the vertex itself.
      label.addPrep(id);
      // It's no longer the frontier.
      value.removeLabel(ConstantLabels.IS_FRONTIER);
      // Emit it out.
      collector.collect(key, value);
      // Notify neighbors.
      Iterator<AbstractEdge> iter=(Iterator<AbstractEdge>) value.getIncidentElements();
      // Create the notifier.
      LabeledAdjVertex notifier =(LabeledAdjVertex) value.clone();
      // Delete useless info.
      notifier.setEdges(new ArrayList<Edge>());      while(iter.hasNext()) {  // Emit all edges this vertex points to.
        collector.collect(new Text(((Edge)iter.next()).getTo()), notifier);
      }    }else{
      // Visited non-frontier vertexes, or non-visited vertexes.
      collector.collect(key, value);
    }
  }
}

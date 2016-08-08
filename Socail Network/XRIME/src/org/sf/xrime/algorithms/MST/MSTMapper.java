/*
 * Copyright (C) yangyin@BUPT. 2009.
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
package org.sf.xrime.algorithms.MST;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTEdgeStateVariable;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTEdgeStatesLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTTimestampTool;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTVertexInforLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTWEdgesLabel;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.edge.WeightOfEdge;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * This class provides some methods used by the mapper class
 * @author YangYin
 */
class MSTMapperAssist{
  /**
   * Method used to initiate the property of vertexes
   * @param labeledAdjVertex
   * @param autoWakeUpList
   */
  public static void addVertexInforLabel(LabeledAdjVertex labeledAdjVertex, String autoWakeUpList)
  {
    MSTVertexInforLabel mstVertexInforLabel = new MSTVertexInforLabel();
    /**
     * Add Auto WakeUp Property for related vertexes
     * If the autoWakeUpList is null, then set all the vertexes to be autoWakeUp
     */
    if(autoWakeUpList == null)
      mstVertexInforLabel.setAutoWakeUp(true);
    else {
      String[] autoWakeUpListVal = autoWakeUpList.split(" ");
      boolean exist = false;
      /**
       * Check whether the current vertex is in the autoWakeUpList
       */
      for(int i = 0; i < autoWakeUpListVal.length; i++)
        if(autoWakeUpListVal[i].equals(labeledAdjVertex.getId()))
          exist = true;
    
      if(exist)
        mstVertexInforLabel.setAutoWakeUp(true);
      else
        mstVertexInforLabel.setAutoWakeUp(false);
    }
    /**
     * Set other related properties
     */
    mstVertexInforLabel.setBestEdge("");
    mstVertexInforLabel.setBestWeight(new WeightOfEdge(Integer.MAX_VALUE));
    mstVertexInforLabel.setFindCount(0);
    mstVertexInforLabel.setFragIdentity("");
    mstVertexInforLabel.setFragLevel(0);
    mstVertexInforLabel.setInBranch("");
    mstVertexInforLabel.setStatus(-1);
    mstVertexInforLabel.setTestEdge("");
    mstVertexInforLabel.setBestEdgeIdentity("");
    
    labeledAdjVertex.setLabel(MSTVertexInforLabel.mstVertexInforLabel, mstVertexInforLabel);
  }
  /**
   * Method used to initiate the status of vertex's edges
   * @param labeledAdjVertex
   */
  public static void addEdgeStatesLabel(LabeledAdjVertex labeledAdjVertex)
  {
    List<Edge> edgeList = labeledAdjVertex.getEdges();
    MSTEdgeStatesLabel mstEdgeStatesLabel = new MSTEdgeStatesLabel();
    Iterator<Edge> iter = edgeList.iterator();
    while(iter.hasNext()) {
      String curEdgeVertex = ((Edge)iter.next()).getTo();
      /**
       * set state to be basic
       */
      mstEdgeStatesLabel.setEdgeState(curEdgeVertex, new MSTEdgeStateVariable(1));      
    }
    labeledAdjVertex.setLabel(MSTEdgeStatesLabel.mstEdgeStatesLabel, mstEdgeStatesLabel);
  }
}

/**
 * The mapper class for the computing part of MST algorithm
 * At the right beginning, this class is used to initiate the vertexes,
 * but after then, it does nothing but only emit the input
 * @author YangYin
 * @see org.apache.hadoop.mapred.Mapper
 */
public class MSTMapper extends GraphAlgorithmMapReduceBase implements Mapper<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  /**
   *  @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void map(Text key, LabeledAdjVertex value,
      OutputCollector<Text, LabeledAdjVertex> collector, Reporter reporter)
      throws IOException {
    /**
     * re-set time stamp flag
     */
    MSTTimestampTool.tag = 0;
    
    /**
     * process the each key value set
     */
    if((key.toString()).equals(value.getId())) {
      /**
       * If VertexInforLabel has not been added, add it
       */
      if(value.getLabel(MSTVertexInforLabel.mstVertexInforLabel) == null) {
        String autoWakeUpList = context.getParameter(MSTStep.autoWakeUpListKey);
        MSTMapperAssist.addVertexInforLabel(value, autoWakeUpList);
      }
      /**
       * If MSTEdgeStatesLabel has not been added, add it
       */
      if(value.getLabel(MSTEdgeStatesLabel.mstEdgeStatesLabel) == null)
        MSTMapperAssist.addEdgeStatesLabel(value);
      
      /**
       * get the infor_label for vertex
       */
      MSTVertexInforLabel mstVertexInforLabel = (MSTVertexInforLabel)value.getLabel(MSTVertexInforLabel.mstVertexInforLabel);
      MSTWEdgesLabel mstWEdgeLabel = (MSTWEdgesLabel)value.getLabel(MSTWEdgesLabel.mstWEdgesLabel);
      MSTEdgeStatesLabel mstEdgeStatesLabel = (MSTEdgeStatesLabel)value.getLabel(MSTEdgeStatesLabel.mstEdgeStatesLabel);
      /**
       * Go to the auto wake up procedure if the vertex should wake up automatically
       */
      if(mstVertexInforLabel.getAutoWakeUp() == true &&  mstVertexInforLabel.getStatus() == -1)
        MSTTool.wakeUp(key.toString(), mstVertexInforLabel, mstWEdgeLabel, mstEdgeStatesLabel, collector);
    }
    collector.collect(key, value);
  }
}
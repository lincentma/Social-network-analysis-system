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
package org.sf.xrime.algorithms.MST.MSTLabel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.edge.WeightOfEdge;
import org.sf.xrime.model.label.Labels;

/**
 * This label is used to accommodate the weight of a certain vertex's edges in MST algorithm
 * @author YangYin
 * @see org.apache.hadoop.io.Writable
 * @see org.sf.xrime.model.edge.WeightOfEdge
 */
public class MSTWEdgesLabel implements Writable, Cloneable {
    
  static final public String mstWEdgesLabel = "xrime.algorithm.MST.wedges.label";
  
  /**
   * The accommodation of edges' weight in MST algorithm are implemented by Labels,
   * and the variable of weight is implemented by MSTEdgeStateVariable
   * @see org.sf.xrime.algorithms.MST.MSTLabel.MSTEdgeStateVariable
   */
  private Labels wEdges;
  
  static {
    WritableFactories.setFactory
          (MSTWEdgesLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new MSTWEdgesLabel(); }
           });
  }
  
  public MSTWEdgesLabel() {
    // TODO Auto-generated constructor stub
    wEdges = new Labels();
  }

  public MSTWEdgesLabel(MSTWEdgesLabel mstWEdgesLabel) {
      wEdges = new Labels(mstWEdgesLabel.getWEdges());
  }
  
  public void setWEdges(Labels edges) {
    wEdges = edges;
  }
  
  public WeightOfEdge getWEdge(String vertexId)
  {
    if(wEdges == null)
      return null;
    else
      return (WeightOfEdge)wEdges.getLabel(vertexId);
  }
  
  public Labels getWEdges()
  {
    return wEdges;
  }
  
  public void addWEdge(String vertexId, WeightOfEdge weight)
  {
    if(wEdges == null)
      wEdges = new Labels();
    wEdges.setLabel(vertexId, weight);
  }
  
  public void addWEdges(Labels extraWEdges)
  {
    if(wEdges == null)
      wEdges = new Labels();
    
    if(extraWEdges == null)
      return;
    
    Set<String> keySet = extraWEdges.getLabels().keySet();
    Iterator<String> iter = keySet.iterator();
    while(iter.hasNext())
    {
      String key = (String) iter.next();
      WeightOfEdge value = (WeightOfEdge) extraWEdges.getLabel(key);
      wEdges.setLabel(key, value);
    }
  }
  
  public String toString() {
    String ret = wEdges.toString();
    return ret;
  }
  
  public Object clone() {
    return new MSTWEdgesLabel(this);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    wEdges.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    wEdges.write(out);
  }

}

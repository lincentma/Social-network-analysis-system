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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.edge.WeightOfEdge;

/**
 * Label for MST 'Report' message
 * This label carries two parameter which are:
 *   the value of the current vertex's best weight,
 *   the identity of the current vertex.
 * @author YangYin
 * @see org.apache.hadoop.io.Writable
 */
public class MSTMessageReportLabel implements Cloneable, Writable {

  static final public String mstMessageReportLabel = "xrime.algorithem.MST.message.report.label";
    
  /**
   * The value of the current vertex's best weight which is
   * implemented by WeightOfEdge
   * @see org.sf.xrime.model.edge.WeightOfEdge
   */   
  private WeightOfEdge  bestWeight;
  
  /**
   * The identity of the current vertex
   */
  private String edgeIdentity;
  
  public MSTMessageReportLabel() {
    // TODO Auto-generated constructor stub
    bestWeight = new WeightOfEdge(-1);
  }
  
  public MSTMessageReportLabel(MSTMessageReportLabel mstMessageReportLabel) {
    bestWeight = mstMessageReportLabel.getBestWeight();
  }
  
  static {
    WritableFactories.setFactory
          (MSTMessageReportLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new MSTMessageReportLabel(); }
           });
    }
  
  public WeightOfEdge getBestWeight() {
    return bestWeight;
  }

  public void setBestWeight(WeightOfEdge bestWeight) {
    this.bestWeight = bestWeight;
  }

  
  public String getEdgeIdentity() {
    return edgeIdentity;
  }

  public void setEdgeIdentity(String edgeIdentity) {
    this.edgeIdentity = edgeIdentity;
  }

  public Object clone() {
    return new MSTMessageReportLabel(this);
  }
    
  public String toString() {
    String ret = "<";
      
    ret = ret + bestWeight;
    ret = ret + ", " + edgeIdentity;
    
    ret = ret + ">";
      
    return ret;
  }
    
  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    bestWeight.readFields(in);
    edgeIdentity = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    bestWeight.write(out);
    Text.writeString(out, edgeIdentity);
  }

}

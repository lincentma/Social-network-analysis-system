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
 * This label is used to accommodate the status of a certain vertex in MST algorithm
 * @author YangYin
 * @see org.apache.hadoop.io.Writable
 */
public class MSTVertexInforLabel implements Cloneable, Writable {

  static final public String mstVertexInforLabel = "xrime.algorithem.MST.vertex.infor.label";
  
  /**
   *  Variable used to indicate whether this vertex will spontaneously wake up or not
   */
  private boolean autoWakeUp = false; // false, not autoWakeUp
                     // true, autoWakeUp
  /**
   * Vertex State which has three different values indicating three different status which are:
   *   -1:"Sleep"
   *      0:"Find"
   *     1:"Found"
   */
  private int status = 0;  
  
  /**
   * Fragment Identity is constructed by the two vertexes' id of the related core edge,
   * such as A_B
   * this requires that the vertexes'id are all unique
   */
  private String fragIdentity = "";
  
  /**
   * Level of the fragment
   */
  private int fragLevel = 0;

  /**
   * Best edge (edge with minimum edge weight)currently
   */
  private String bestEdge = ""; 
  
  /**
   * Best weight, the minimum weight found currently
   */
  private WeightOfEdge bestWeight;
  
  /**
   * Test edge, the edge that is currently sending "Test" message
   */
  private String testEdge = "";
  
  /**
   * InBranch, the edge that has sent initial message to the current node
   */
  private String inBranch = "";
  
  /**
   * Identity of the best edge which is made up of the edge's related vertexes' id
   */
  private String bestEdgeIdentity = "";
  
  /**
   * Find count, the total number of initial messages that have been sent by the current vertex
   * Whenever the current node receives a report message, the find count will subtract one,
   * and when find count reaches zero, it means that the current node has collected all the response
   * report messages, and then it can send its own report message
   */
  private int findCount = 0;
  
  public MSTVertexInforLabel(MSTVertexInforLabel mstVertexInforLabel) {
    autoWakeUp = mstVertexInforLabel.getAutoWakeUp();
    status = mstVertexInforLabel.getStatus();
    fragIdentity = mstVertexInforLabel.getFragIdentity();
    fragLevel = mstVertexInforLabel.getFragLevel();
    bestEdge = mstVertexInforLabel.getBestEdge();
    bestWeight = mstVertexInforLabel.getBestWeight();
    testEdge = mstVertexInforLabel.getTestEdge();
    inBranch = mstVertexInforLabel.getInBranch();
    findCount = mstVertexInforLabel.getFindCount();
  }
  
  static {
    WritableFactories.setFactory
          (MSTVertexInforLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new MSTVertexInforLabel(); }
           });
    }
    
  public MSTVertexInforLabel() {
    // TODO Auto-generated constructor stub
    bestWeight = new WeightOfEdge(Integer.MAX_VALUE);
  }
  
  
  public boolean getAutoWakeUp() {
    return autoWakeUp;
  }


  public void setAutoWakeUp(boolean autoWakeUp) {
    this.autoWakeUp = autoWakeUp;
  }


  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public String getFragIdentity() {
    return fragIdentity;
  }

  public void setFragIdentity(String fragIdentity) {
    this.fragIdentity = fragIdentity;
  }

  public int getFragLevel() {
    return fragLevel;
  }

  public void setFragLevel(int fragLevel) {
    this.fragLevel = fragLevel;
  }

  public String getBestEdge() {
    return bestEdge;
  }

  public void setBestEdge(String bestEdge) {
    this.bestEdge = bestEdge;
  }

  public WeightOfEdge getBestWeight() {
    return bestWeight;
  }

  public void setBestWeight(WeightOfEdge bestWeight) {
    this.bestWeight = (WeightOfEdge)bestWeight.clone();
  }

  public String getTestEdge() {
    return testEdge;
  }

  public void setTestEdge(String testEdge) {
    this.testEdge = testEdge;
  }

  public String getInBranch() {
    return inBranch;
  }

  public void setInBranch(String inBranch) {
    this.inBranch = inBranch;
  }

  public int getFindCount() {
    return findCount;
  }

  public void setFindCount(int findCount) {
    this.findCount = findCount;
  }

  public Object clone() {
    return new MSTVertexInforLabel(this);
  }
  
  public String getBestEdgeIdentity() {
    return bestEdgeIdentity;
  }


  public void setBestEdgeIdentity(String bestEdgeIdentity) {
    this.bestEdgeIdentity = bestEdgeIdentity;
  }


  public String toString() {
    String ret = "<";
    
    ret = ret + autoWakeUp + ", " + status + ", " + fragIdentity + ", " + fragLevel + ", " + 
    bestEdge + ", " + bestWeight + ", " + testEdge + ", " + inBranch + ", " + bestEdgeIdentity + ", " + findCount;
    
    ret = ret + ">";
    
    return ret;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    autoWakeUp = in.readBoolean();
    status = in.readInt();
    fragIdentity = Text.readString(in);
    fragLevel = in.readInt();
    bestEdge = Text.readString(in);
    bestWeight.readFields(in);
    testEdge = Text.readString(in);
    inBranch = Text.readString(in);
    bestEdgeIdentity = Text.readString(in);
    findCount = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    out.writeBoolean(autoWakeUp);
    out.writeInt(status);
    Text.writeString(out, fragIdentity);
    out.writeInt(fragLevel);
    Text.writeString(out, bestEdge);
    bestWeight.write(out);
    Text.writeString(out, testEdge);
    Text.writeString(out, inBranch);
    Text.writeString(out, bestEdgeIdentity);
    out.writeInt(findCount);
  }

}

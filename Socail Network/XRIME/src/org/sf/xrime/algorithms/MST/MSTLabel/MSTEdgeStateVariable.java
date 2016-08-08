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

import org.apache.hadoop.io.Writable;


/**
 * This writable class is used to indicate the real value of a edge's status. 
 * @author YangYin
 *  @see org.apache.hadoop.io.Writable
 */
public class MSTEdgeStateVariable implements Cloneable, Writable {
  
  /**
   * This variable can have three different values indicating three different
   * status which are: -1, Branch
   *                0, Rejected
   *               1, Basic
  */
  private int state;
  
  public MSTEdgeStateVariable() {
    state  = 0;
  }
  
  public MSTEdgeStateVariable(int state) {
    this.state = state;
  }
  
  public MSTEdgeStateVariable(MSTEdgeStateVariable weightOfEdge) {
    this.state = weightOfEdge.getState();
  }

  public int getState() {
    return state;
  }

  public void setState(int state) {
    this.state = state;
  }
  
  public Object clone() {
    return new MSTEdgeStateVariable(this);
  }
  
  public String toString(){
    String ret = "";
    ret = ret + state;
    return ret;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // Clear the container.
    state = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(state);
  }
}

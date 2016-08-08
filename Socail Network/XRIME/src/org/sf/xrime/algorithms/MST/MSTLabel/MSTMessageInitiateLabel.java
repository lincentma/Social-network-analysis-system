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

/**
 * Label for MST 'Initiate' message
 * This label carries three parameter which are:
 *   the level of the current vertex's located fragment,
 *   the identity of the current vertex's located fragment,
 *   the status of the current vertex.
 * @author YangYin
 * @see org.apache.hadoop.io.Writable
 */
public class MSTMessageInitiateLabel implements Cloneable, Writable {

  static final public String mstMessageInitiateLabel = "xrime.algorithem.MST.message.initiate.label";
  
  /**
   * The level of the current vertex's located fragment
   */
  private int  fragLevel = 0;
  
  /**
   * The identity of the current vertex's located fragment
   */
  private String fragIdentity = "";
  
  /**
   * The status of the current vertex
   */
  private int state = 0;
  
  public MSTMessageInitiateLabel() {
    // TODO Auto-generated constructor stub
    
  }
  
  public MSTMessageInitiateLabel(MSTMessageInitiateLabel mstMessageInitiateLabel) {
    fragLevel = mstMessageInitiateLabel.getFragLevel();
    fragIdentity = mstMessageInitiateLabel.getFragIdentity();
    state = mstMessageInitiateLabel.getState();
  }
  
  static {
    WritableFactories.setFactory
          (MSTMessageInitiateLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new MSTMessageInitiateLabel(); }
           });
    }
    
  public int getFragLevel() {
    return fragLevel;
  }

  public void setFragLevel(int fragLevel) {
    this.fragLevel = fragLevel;
  }

  public String getFragIdentity() {
    return fragIdentity;
  }

  public void setFragIdentity(String fragIdentity) {
    this.fragIdentity = fragIdentity;
  }

  public int getState() {
    return state;
  }

  public void setState(int state) {
    this.state = state;
  }

  public Object clone() {
    return new MSTMessageInitiateLabel(this);
  }
  
  public String toString() {
    String ret = "<";
    
    ret = ret + fragLevel + ", " + fragIdentity + ", " + state;
    
    ret = ret + ">";
    
    return ret;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    fragLevel = in.readInt();
    fragIdentity = Text.readString(in);
    state = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    out.writeInt(fragLevel);
    Text.writeString(out, fragIdentity);
    out.writeInt(state);
  }

}

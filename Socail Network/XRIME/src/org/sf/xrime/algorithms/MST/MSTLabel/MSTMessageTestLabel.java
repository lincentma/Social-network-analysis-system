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
 * Label for MST 'Test' message
 * This label carries two parameter which are:
 *   the level of the current vertex's located fragment,
 *   the identity of the current vertex's located fragment.
 * @author YangYin
 * @see org.apache.hadoop.io.Writable
 */
public class MSTMessageTestLabel implements Cloneable, Writable {

  static final public String mstMessageTestLabel = "xrime.algorithem.MST.message.test.label";
  
  /**
   * the level parameter in the initiate message
   */
  private int  fragLevel = 0;
  
  /**
   * the fragment identity in the initiate message
   */
  private String fragIdentity = "";
    
  public MSTMessageTestLabel() {
    // TODO Auto-generated constructor stub
    
  }
  
  public MSTMessageTestLabel(MSTMessageTestLabel mstMessageTestLabel) {
    fragLevel = mstMessageTestLabel.getFragLevel();
    fragIdentity = mstMessageTestLabel.getFragIdentity();
  }
  
  static {
    WritableFactories.setFactory
          (MSTMessageTestLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new MSTMessageTestLabel(); }
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

  public Object clone() {
    return new MSTMessageTestLabel(this);
  }
  
  public String toString() {
    String ret = "<";
    
    ret = ret + fragLevel + ", " + fragIdentity;
    
    ret = ret + ">";
    
    return ret;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    fragLevel = in.readInt();
    fragIdentity = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    out.writeInt(fragLevel);
    Text.writeString(out, fragIdentity);
  }

}

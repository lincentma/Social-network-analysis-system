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
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Label for MST 'ChangeRoot' message
 * This label doesn't carry any parameters
 * @author YangYin
 * @see org.apache.hadoop.io.Writable
 */
public class MSTMessageChangeRootLabel implements Cloneable, Writable {

  static final public String mstMessageChangeRootLabel = "xrime.algorithem.MST.message.changeroot.label";
    
  /**
   * this label has no parameter to carry
   */
    
  public MSTMessageChangeRootLabel() {
    // TODO Auto-generated constructor stub
  }
  
  public MSTMessageChangeRootLabel(MSTMessageChangeRootLabel mstMessageChangeRootLabel) {
  
  }
  
  static {
    WritableFactories.setFactory
          (MSTMessageChangeRootLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new MSTMessageChangeRootLabel(); }
           });
    }
    
  public Object clone() {
    return new MSTMessageChangeRootLabel(this);
  }
    
  public String toString() {
    String ret = "<";
    ret = ret + ">";
      
    return ret;
  }
    
  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub

  }

}

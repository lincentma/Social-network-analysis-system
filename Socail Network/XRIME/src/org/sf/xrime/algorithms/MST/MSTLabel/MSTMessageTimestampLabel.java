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
 * Label for time stamp of MST messages 
 * In order to differentiate the time sequencing of messages, every messages in
 * MST algorithm will be put a time stamp label
 * This label carries one parameter which is:
 *   the time stamp of the current time when this message is created.
 * @author YangYin
 * @see org.apache.hadoop.io.Writable
 * @see org.sf.xrime.algorithms.MST.MSTLabel.MSTTimestampTool
 */
public class MSTMessageTimestampLabel implements Cloneable, Writable {
  
  static final public String mstMessageTimestampLabel = "xrime.algorithem.MST.message.timestamp.label";
  
  /**
   * the timeStamp parameter in the initiate message
   */
  private String  timeStamp = "";
    
  public MSTMessageTimestampLabel() {
    // TODO Auto-generated constructor stub
    
  }
  
  public MSTMessageTimestampLabel(MSTMessageTimestampLabel mstMessageTimestampLabel) {
    timeStamp = mstMessageTimestampLabel.getTimeStamp();
  }
  
  static {
    WritableFactories.setFactory
          (MSTMessageTimestampLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new MSTMessageTimestampLabel(); }
           });
  }
    
  public String getTimeStamp() {
    return timeStamp;
  }

  public void setTimeStamp(String timeStamp) {
    this.timeStamp = timeStamp;
  }

  public Object clone() {
    return new MSTMessageTimestampLabel(this);
  }
  
  public String toString() {
    String ret = "<";
    
    ret = ret + timeStamp;
    
    ret = ret + ">";
    
    return ret;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    timeStamp = Text.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    Text.writeString(out, timeStamp);
  }

}

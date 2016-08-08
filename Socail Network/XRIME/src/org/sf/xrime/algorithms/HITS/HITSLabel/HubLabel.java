/*
 * Copyright (C) quna@BUPT. 2009.
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
package org.sf.xrime.algorithms.HITS.HITSLabel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

public class HubLabel implements Cloneable, Writable {
  static private DecimalFormat format = new DecimalFormat("#0.00000000");
  
  private double hubscore;
  private double prehubscore;
  
  static {
    WritableFactories.setFactory
          (HubLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new HubLabel(); }
           });
    }
  
  public HubLabel(){
    hubscore = 1;
    prehubscore = 0;
  }
  
  public HubLabel(double hubscore){
    this.hubscore = hubscore;
    this.prehubscore = 0;
  }
  
  public HubLabel(HubLabel hubscore){
    this.hubscore = hubscore.getHubscore();
    this.prehubscore = hubscore.getPreHubscore();
  }
  
  public double getHubscore(){
    return hubscore;
  }
  
  public void setHubscore(double hubscore){
    this.hubscore = hubscore;
  }
  
  public double getPreHubscore(){
    return prehubscore;
  }
  
  public void setPreHubscore(double prehubscore){
    this.prehubscore = prehubscore;
  }
  
  public String toString() {    
    String ret = "<";
    ret = ret + format.format(prehubscore) + "," + format.format(hubscore);
    return ret+">";
  }
  
  public Object clone() {
    return new HubLabel(this);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    hubscore=in.readDouble();
    prehubscore=in.readDouble();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(hubscore);
    out.writeDouble(prehubscore);
  }
}

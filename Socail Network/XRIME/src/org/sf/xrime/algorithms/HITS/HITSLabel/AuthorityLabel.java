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

public class AuthorityLabel implements Cloneable, Writable {
  static private DecimalFormat format = new DecimalFormat("#0.00000000");
  
  private double authorityscore;
  private double preauthorityscore;
  
  static {
    WritableFactories.setFactory
          (AuthorityLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new AuthorityLabel(); }
           });
    }
  
  public AuthorityLabel(){
    authorityscore = 1;
    preauthorityscore = 0;
  }
  
  public AuthorityLabel(double authorityscore){
    this.authorityscore = authorityscore;
    this.preauthorityscore = 0;
  }
  
  public AuthorityLabel(AuthorityLabel authorityscore){
    this.authorityscore = authorityscore.getAuthorityscore();
    this.preauthorityscore = authorityscore.getPreAuthorityscore();
  }
  
  public double getAuthorityscore(){
    return authorityscore;
  }
  
  public void setAuthorityscore(double authorityscore){
    this.authorityscore = authorityscore;
  }
  
  public double getPreAuthorityscore(){
    return preauthorityscore;
  }
  
  public void setPreAuthorityscore(double preauthorityscore){
    this.preauthorityscore = preauthorityscore;
  }
  
  public String toString() {    
    String ret = "<";
    ret = ret + format.format(preauthorityscore) + "," + format.format(authorityscore);
    return ret+">";
  }
  
  public Object clone() {
    return new AuthorityLabel(this);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    authorityscore=in.readDouble();
    preauthorityscore=in.readDouble();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(authorityscore);
    out.writeDouble(preauthorityscore);
  }
}

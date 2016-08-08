/*
 * Copyright (C) IBM Corp. 2009.
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
package org.sf.xrime.algorithms.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Writable;

/**
 * Label for Prior PageRank. Here PageRank score, previous PageRank score, 
 * whether this node can be reached from initial vertex and whether this node is initial vertex
 * are recorded.
 * 
 * @author Cai Bin
 */
public class PageRankLabel implements Cloneable, Writable {
  static final public String pageRankLabelKey = "xrime.algorithm.pageRank.label";
  
  static private DecimalFormat format=new DecimalFormat("#0.0000000");  
  
  /**
   * Page rank score.
   */
  private double pr;
  
  /**
   * weight for re-distribute page rank score.
   */
  private double initWeight;
  
  /**
   * Previous page rank score.
   */
  private double prepPR;
  
  /**
   * Whether this node can be reached from initial vertex.
   */
  private boolean reachable;
  
  /**
   * Whether this node is initial vertex.
   */
  private boolean initVertex;
  
  public PageRankLabel() {
    pr=1;
    prepPR=0;
    reachable=false;
    initVertex=false;
  }
  
  public PageRankLabel(double pr) {
    this.pr=pr;
    prepPR=0;
    reachable=false;
    initVertex=false;
  }
  
  public PageRankLabel(
      PageRankLabel pageRankWithPriorsLabel) { 
    this.pr=pageRankWithPriorsLabel.getPr();
    this.prepPR=pageRankWithPriorsLabel.getPrepPR();
    this.reachable=pageRankWithPriorsLabel.isReachable();
    this.initVertex=pageRankWithPriorsLabel.isInitVertex();
  }
  
  public double getPr() {
    return pr;
  }

  public void setPr(double pr) {
    this.pr = pr;
  }

  public double getInitWeight() {
    return initWeight;
  }

  public void setInitWeight(double initWeight) {
    this.initWeight = initWeight;
  }
  
  public double getPrepPR() {
    return prepPR;
  }

  public void setPrepPR(double prepPR) {
    this.prepPR = prepPR;
  }


  public boolean isReachable() {
    return reachable;
  }

  public void setReachable(boolean reachable) {
    this.reachable = reachable;
  }

  public boolean isInitVertex() {
    return initVertex;
  }

  public void setInitVertex(boolean initVertex) {
    this.initVertex = initVertex;
  }
  
  public String toString() {
    String ret=format.format(pr);
    
    if(reachable) {
      ret+=", V";
    } else {
      ret+=", U";
    }
    
    if(initVertex) {
      ret+=", I";
    } else {
      ret+=", U";
    }
    
    return ret;
  }
  
  public Object clone() {
    return new PageRankLabel(this);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    pr=in.readDouble();
    initWeight=in.readDouble();
    prepPR=in.readDouble();
    reachable=in.readBoolean();
    initVertex=in.readBoolean();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(pr);
    out.writeDouble(initWeight);
    out.writeDouble(prepPR);
    out.writeBoolean(reachable);
    out.writeBoolean(initVertex);
  }
}

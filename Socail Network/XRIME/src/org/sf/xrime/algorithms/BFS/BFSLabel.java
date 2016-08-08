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
package org.sf.xrime.algorithms.BFS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Label for BFS algorithm. 
 * <p>
 * This label includes status of the vertex and one shortest vertex list from the root to this nodes (there might be
 * more than one shortest vertex list, we only take one).
 * @author Cai Bin
 */
public class BFSLabel implements Cloneable, Writable {
  static final public String bfsLabelPathsKey = "xrime.algorithm.BFS.label";
  
  /**
   * The shortest vertex list from the root to this nodes.
   */
  private List<String> preps;
  
  /**
   * status of the vertex. -1, root vertex; 0, not visited vertex; 1, visited vertex.
   */
  private int status;  // -1, init value for starting vertex
                       // 0,  not visited value for search
                       // 1,  visited value for search
  
  static {
    // Register the factory.
    WritableFactories.setFactory
          (BFSLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new BFSLabel(); }
           });
  }
  
  public BFSLabel() {
    status=0;
    preps=new ArrayList<String>();
  }
  
  public BFSLabel(BFSLabel bfsLabel) {
    status=bfsLabel.getStatus();
    preps=new ArrayList<String>(bfsLabel.getPreps());
  }
    
  public List<String> getPreps() {
    return preps;
  }

  public void setPreps(List<String> preps) {
    this.preps = preps;
  }

  public void addPrep(String vertex) {
    if(preps==null) {
      preps=new ArrayList<String>();
    }
    
    preps.add(vertex);
  }

  public void addPreps(List<String> preps) {
    if(this.preps==null) {
      this.preps=new ArrayList<String>();
    }
    
    this.preps.addAll(preps);
  }
  
  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }
    
  public String toString() {
    String ret="<";
    switch(status) {
    case -1:
      ret+="I";
      break;
    case  0:
      ret+="U";
      break;
    case  1:
      ret+="V";
      break;      
    }
    
    if(preps.size()>0) {
      ret+=", <";
      for(String prep : preps) {
          ret += prep+", ";
      }
      ret = ret.substring(0, ret.length() - 2) + ">";
    }
    
    return ret+">";
  }
  
  public Object clone() {
    return new BFSLabel(this);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    status=in.readInt();
      
    int size=in.readInt();
    if(preps==null) {
      preps=new ArrayList<String>();
    } else {
      preps.clear();
    }
    
    for(int ii=0;ii<size;ii++) {
      String item=Text.readString(in);
      preps.add(item);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(status);
      
    if(preps==null) {
      out.writeInt(0);
      return;
    }

    out.writeInt(preps.size());
    for(String item : preps) {
      Text.writeString(out, item);
    }
  }
}

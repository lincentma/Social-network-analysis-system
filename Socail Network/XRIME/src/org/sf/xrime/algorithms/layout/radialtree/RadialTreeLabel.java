/*
 * Copyright (C) liuchangyan@BUPT. 2009.
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
package org.sf.xrime.algorithms.layout.radialtree;

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
 * Label for RadialTree Layout algorithm. 
 * 
 * This label includes status of the vertex, weight of vertex, coordinates of vertex, angle
 * scale of vertex(from, to, begin) and its successive vertices, distance from root, a token 
 * of having successive vertices, previous vertex of this vertex, previous vertex's distance 
 * from root, the number of successive vertices, one possible vertex list from the root 
 * to this vertex and one possible vertex list begin from this vertex.
 * @author liu chang yan
 */
public class RadialTreeLabel implements Cloneable, Writable {
  static final public String RadialTreeLabelPathsKey = "xrime.algorithm.RadialTree.layout.label";

  /**
   * Possible vertex list from the root to this nodes.
   */
  private List<String> preps;
  /**
   * Possible vertex list begin from this vertex
   */
  private List<String> succ;
  /**
   * status of the vertex. -1, root vertex; 0, not visited vertex; 1, visited vertex.
   */
  private int status;  // -1, init value for starting vertex
                       // 0,  not visited value for search
                       // 1,  visited value for search
  private double weight;  // weight of vertex
  private int coordinate_x;
  private int coordinate_y;
  
  // angle scale of vertex and its successive vertices
  private double angle_from;
  private double angle_to;
  private double angle_begin;
  private double succ_angle_from;
  private double succ_angle_to;
  
  private long distance;  // distance from root
  private int end;  // 0, vertex has successive vertices
            // 1, vertex does not have successive vertices
  private String pre;
  private long predistance;
  private long next;
  
  static {
    WritableFactories.setFactory
          (RadialTreeLabel.class,
           new WritableFactory() {
               public Writable newInstance() { return new RadialTreeLabel(); }
           });
    }
  
    public RadialTreeLabel() {
      status = 0;
      weight = 0;
      coordinate_x = 0;
      coordinate_y = 0;
      angle_from = 0;
      angle_to = 0;
      angle_begin = 0;
      succ_angle_from = 0;
      succ_angle_to = 0;
      distance = 0;
      end = 0;
      preps = new ArrayList<String>();
      pre = new String();
      predistance = 0;
      next = 0;
      succ = new ArrayList<String>();
    }
  
    public RadialTreeLabel(RadialTreeLabel radialTreeLabel) {
      status = radialTreeLabel.getStatus();
      weight = radialTreeLabel.getWeight();
      coordinate_x = radialTreeLabel.getCoordinate_x();
      coordinate_y = radialTreeLabel.getCoordinate_y();
      angle_from = radialTreeLabel.getAngle_from();
      angle_to = radialTreeLabel.getAngle_to();
      angle_begin = radialTreeLabel.getAngle_begin();
      succ_angle_from = radialTreeLabel.getSucc_angle_from();
      succ_angle_to = radialTreeLabel.getSucc_angle_to();
      distance = radialTreeLabel.getDistance();
      end = radialTreeLabel.getEnd();
      preps = new ArrayList<String>(radialTreeLabel.getPreps());
      pre = radialTreeLabel.getPre();
      predistance = radialTreeLabel.getPredistance();
      next = radialTreeLabel.getNext();
      succ = new ArrayList<String>(radialTreeLabel.getSucc());
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
      
  public int getCoordinate_x() {
    return coordinate_x;
  }

  public void setCordinate_x(int coordinate_x) {
    this.coordinate_x = coordinate_x;
  }
  
  public int getCoordinate_y() {
    return coordinate_y;
  }

  public void setCordinate_y(int coordinate_y) {
    this.coordinate_y = coordinate_y;
  }
  
  public double getAngle_from() {
    return angle_from;
  }

  public void setAngle_from(double angle_from) {
    this.angle_from = angle_from;
  }
  
  public double getAngle_to() {
    return angle_to;
  }

  public void setAngle_to(double angle_to) {
    this.angle_to = angle_to;
  }
  
  public double getAngle_begin() {
    return angle_begin;
  }

  public void setAngle_begin(double angle_begin) {
    this.angle_begin = angle_begin;
  }
  
  public double getSucc_angle_from() {
    return succ_angle_from;
  }

  public void setSucc_angle_from(double succ_angle_from) {
    this.succ_angle_from = succ_angle_from;
  }
  
  public double getSucc_angle_to() {
    return succ_angle_to;
  }

  public void setSucc_angle_to(double succ_angle_to) {
    this.succ_angle_to = succ_angle_to;
  }
  
  public int getEnd() {
    return end;
  }

  public void setEnd(int end) {
    this.end = end;
  }
  
  public double getWeight() {
    return weight;
  }

  public void setDistance(long distance) {
    this.distance = distance;
  }
  
  public long getDistance() {
    return distance;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }
  
  public String getPre() {
    return pre;
  }

  public void setPre(String pre) {
    this.pre = pre;
  }
  
  public void setPredistance(long predistance) {
    this.predistance = predistance;
  }
  
  public long getPredistance() {
    return predistance;
  }
  
  public void setNext(long next) {
    this.next = next;
  }
  
  public long getNext() {
    return next;
  }
  
  public List<String> getSucc() {
    return succ;
  }

  public void setSucc(List<String> succ) {
    this.succ = succ;
  }

  public void addSucc(String vertex) {
    if(succ==null) {
      succ=new ArrayList<String>();
    }
    
    succ.add(vertex);
  }

  public void addSucc(List<String> succ) {
    if(this.succ==null) {
      this.succ=new ArrayList<String>();
    }
    
    this.succ.addAll(succ);
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
    ret += ", ";
    ret += weight;
    ret += ", ";
    ret += coordinate_x;
    ret += ", ";
    ret += coordinate_y;
    ret += ", ";
    ret += angle_from;
    ret += ", ";
    ret += angle_to;
    ret += ", ";
    ret += angle_begin;
    ret += ", ";
    ret += distance;
    ret += ", ";
    ret += end;
    ret += ", ";
    ret += pre;
    ret += ", ";
    ret += predistance;
    ret += ", ";
    ret += next;
    ret += ", ";
    ret += succ_angle_from;
    ret += ", ";
    ret += succ_angle_to;
    if(preps.size() > 0) {
      ret += ", <";
      for(String prep : preps) {
          ret += prep+", ";
      }
      ret = ret.substring(0, ret.length() - 2) + ">";
    }
    if(succ.size() > 0) {
      ret += ", <";
      for(String successive : succ) {
          ret += successive+", ";
      }
      ret = ret.substring(0, ret.length() - 2) + ">";
    }
    
    return ret+">";
  }
  
  public Object clone() {
    return new RadialTreeLabel(this);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
      status = in.readInt();
      weight = in.readDouble();
      coordinate_x = in.readInt();
      coordinate_y = in.readInt();
      angle_from = in.readDouble();
      angle_to = in.readDouble();
      angle_begin = in.readDouble();
      distance = in.readLong();
      end = in.readInt();
      pre = Text.readString(in);
      predistance = in.readLong();
      next = in.readLong();
      succ_angle_from = in.readDouble();
      succ_angle_to = in.readDouble();
      int size1=in.readInt();            
    if(preps==null) {
      preps=new ArrayList<String>();
    } else {
      preps.clear();
    }
    
      for(int ii=0;ii<size1;ii++) {
        String item=Text.readString(in);
        preps.add(item);
      }
      
      int size2=in.readInt();            
    if(succ==null) {
      succ=new ArrayList<String>();
    } else {
      succ.clear();
    }
    
      for(int ii=0;ii<size2;ii++) {
        String item=Text.readString(in);
        succ.add(item);
      }
  }

  @Override
  public void write(DataOutput out) throws IOException {
        out.writeInt(status);
      out.writeDouble(weight);
      out.writeInt(coordinate_x);
      out.writeInt(coordinate_y);
      out.writeDouble(angle_from);
      out.writeDouble(angle_to);
      out.writeDouble(angle_begin);
      out.writeLong(distance);
      out.writeInt(end);
      Text.writeString(out, pre);
      out.writeLong(predistance);
      out.writeLong(next);
      out.writeDouble(succ_angle_from);
      out.writeDouble(succ_angle_to);
      if(preps==null) {
        out.writeInt(0);
      }else{
        out.writeInt(preps.size());
        for(String item : preps) {
          Text.writeString(out, item);
        }
      }
       if(succ==null) {
         out.writeInt(0);
       }else{
         out.writeInt(succ.size());
         for(String item : succ) {
           Text.writeString(out, item);
         }
       }
  }
}

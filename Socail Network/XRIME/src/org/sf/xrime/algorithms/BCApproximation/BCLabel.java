/*
 * Copyright (C) yangcheng@BUPT. 2009.
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
package org.sf.xrime.algorithms.BCApproximation;


import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

import org.sf.xrime.model.Element;
import org.sf.xrime.model.label.LabelAdder;
import org.sf.xrime.model.label.Labelable;
/*
 * @author Yang Cheng
 * the label includes the information that is needed in the forward step and backward step
 * 
 */

public class BCLabel implements Cloneable,Writable{
  static final public String bcLabelPathsKey = "xrime.algorithm.BC.label";
  private int status;  // -1, init value for starting vertex
  // 0,  not visited value for search
  // 1,  visited value for search
  private int distance;//distance from the specific source vertex;
  private int number;// the number of shortest paths
  private float bc;//betweenness centrality
  private List<String> precessor;
  

  static{
    WritableFactories.setFactory(BCLabel.class, new WritableFactory(){
      public Writable newInstance()
      {
        return new BCLabel();
      }
    });
  }

  static public class BCLabelAdder extends LabelAdder
  {
    public BCLabelAdder()
    {}

    @Override
    public void addLabels(Labelable labels, Element element) {
      BCLabel label=(BCLabel)labels.getLabel(BCLabel.bcLabelPathsKey);
      if(label==null) {
        labels.setLabel(BCLabel.bcLabelPathsKey,new BCLabel());
      }
    }    
  }


  public BCLabel()
  {
    precessor=new ArrayList<String>();
    distance=-1;
    status=0;
    bc=0;
    number=0;    
  }

  public BCLabel(BCLabel bcLabel)
  {
    status=bcLabel.status;
    this.distance=bcLabel.getDsitance();
    this.bc=bcLabel.getBC();
    this.number=bcLabel.getNumber();
    precessor=bcLabel.getPrecessor();
    

  }

  public float getBC()
  {
    return bc;
  }
  public void setBC(float bc)
  {
    this.bc=bc;
  }

  public int getNumber()
  {
    return number;
  }
  
  public void setNumber(int number)
  {
    this.number=number;
  }
  
  public int getStatus()
  {
    return status;
  }

  public int getDsitance()
  {
    return distance;
  }
  
  public List<String> getPrecessor()
  {
    return precessor;
  }
  

  

  public void setStatus(int status)

  {
    this.status=status;
  }

  public void setDistance(int distance)
  {
    this.distance=distance;
  }


  public void setPrecessor(ArrayList<String> precessor)
  {
    this.precessor=precessor;
  }

  public void addPrecessor(String vert)
  {
    if(precessor==null)
    {
      precessor=new ArrayList<String>();
    }
    precessor.add(vert);
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
    
    ret+=", <"+distance+">";
    ret+=", <"+number+">";
    ret+=", <"+bc+">";
    
    if(precessor.size()>0) {
      ret+=", <";
      for(String prep : precessor) {
          ret += prep+", ";
      }
      ret = ret.substring(0, ret.length() - 2) + ">";
    }
    
    return ret+">";
  }

  public Object clone()
  {
    return new BCLabel(this);
  }

  public void readFields(DataInput in) throws IOException
  {  
     status=in.readInt();
     distance=in.readInt();
     number=in.readInt();
     bc=in.readFloat();
    
     
    int prosize=in.readInt();
    if(prosize!=0)
    {
      if(precessor==null)
      {
        precessor=new ArrayList<String>();
      }
      else
      {
        precessor.clear();
      }
        
      for(int i=0;i<prosize;i++)
      {
        String item=Text.readString(in);
        precessor.add(item);
      }
    }
  }

  public void write(DataOutput out)  throws IOException
  {
    
    out.writeInt(status);
    out.writeInt(distance);
    out.writeInt(number);
    out.writeFloat(bc);
    int prosize=precessor.size();
//    int sucsize=successor.size();
    out.writeInt(prosize);
    if(prosize!=0)
    {
      for(String item : precessor) 
      {
        Text.writeString(out, item);
      }
    }
  }
}

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

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;

/**
 * @author Yang Cheng
 * this program backward the whole graph from the farthest vertexes from the source vertexes
 * a parameter "distance" is read from the configuration of the job, it determines which vertexes are in this round of 
 * reduce action if this vertex's distance is equal to the distance read from the configuration
 */

public class BCBackwardReducer extends GraphAlgorithmMapReduceBase implements Reducer<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{
  public void reduce(Text key,Iterator<LabeledAdjBiSetVertex> values,
    OutputCollector<Text,LabeledAdjBiSetVertex> output,Reporter reporter) throws IOException{  
//    System.out.println("here the execution of back reduce---------------------");
    LabeledAdjBiSetVertex current=null;
    List<LabeledAdjBiSetVertex> successor=new ArrayList<LabeledAdjBiSetVertex>();
    
    float bc=0;
    while(values.hasNext())   
    {
      LabeledAdjBiSetVertex tem=new LabeledAdjBiSetVertex(values.next());
      if(tem.getId().equals(key.toString()))
        current=tem;                                                                  // get the current vertex
      else
        successor.add(tem);                    // others are the current vertex's successor 
    }
    
    BCLabel currentlabel=(BCLabel)current.getLabel(BCLabel.bcLabelPathsKey);
//    System.out.println("in BCReducer the distance is :--------------------------"+Integer.parseInt(context.getProperties().getProperty("distance"))+"$$$$$$$$$$$$$$$$$");
    if(currentlabel.getDsitance()==Integer.parseInt(context.getParameter("distance")))                         // it is the time for this vertex to action
    {
//      System.out.println("distance wei "+Integer.parseInt(context.getProperties().getProperty("distance"))+" de vertex:"+current);
      if(successor.size()!=0)                       //to do the reduce action
      {    
        for(LabeledAdjBiSetVertex item:successor)
        {  
          BCLabel temlabel=(BCLabel)item.getLabel(BCLabel.bcLabelPathsKey);
//          System.out.println("temlabel de bc---------"+temlabel.getBC());
//          System.out.println("templabel de number-------"+temlabel.getNumber());
//          System.out.println("current numer---------"+currentlabel.getNumber());
          bc+=(float)currentlabel.getNumber()/(float)temlabel.getNumber()*(1+temlabel.getBC());        //avoid the loss of precise
//          System.out.println("now bc is============="+bc);
        }
      
      currentlabel.setBC(bc);
//      System.out.println("currentlabel============"+currentlabel);
      }
      output.collect(key, current);           //collect itself
      Text out=new Text();
      List<String> precessor=currentlabel.getPrecessor();             // get its precessor and emit its information to them
      if(precessor.size()!=0)   
      {
        for(String item:precessor)
        {
        out.set(item);
//        System.out.println("distance wei "+Integer.parseInt(context.getProperties().getProperty("distance"))+" emit key:"+item+" value :"+current);
        output.collect(out, current);
        }
      }

    }
    else                                 // not in this round of action , just emit itself
    {

      output.collect(key, current);          //else the vertex is not in the current reduce phase, do nothing;
    }
  }
}



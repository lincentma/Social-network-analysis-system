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
package org.sf.xrime.algorithms.BC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.sf.xrime.algorithms.BC.BCForwardStep;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;

/*
 * @author Yang Cheng
 * to traverse the whole graph from the initial vertex specified by the BCRunner
 * bread first traverse from the source vertex
 * 
 * 
 */
public class BCForwardReducer extends GraphAlgorithmMapReduceBase implements Reducer<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{
  JobConf job=new JobConf();
  boolean changeflag=false;
  
  public void reduce(Text key,Iterator<LabeledAdjBiSetVertex> values,
      OutputCollector<Text,LabeledAdjBiSetVertex> output,Reporter reporter)
  {
//    System.out.println("here forward reducer is executed-----------------------------------");
    List<LabeledAdjBiSetVertex> precessorBCVertex=new ArrayList<LabeledAdjBiSetVertex>();
    //  LabeledAdjBiSetVertex inputBCVertex=null;
    LabeledAdjBiSetVertex currentVertex=null;
    LabeledAdjBiSetVertex temvert=null;
    LabeledAdjBiSetVertex inivert=null;
    Set<String> precessorIdSet=new HashSet<String>();
    Text outputKey=new Text();
    int distance=-1;
    int number=0;

    try{
//    System.out.println("key    "+key+"-----------------");

    if(precessorBCVertex.size()!=0)
      precessorBCVertex.clear();
    while(values.hasNext())
      
    {  
//      System.out.println("next again----------------------");
      temvert=new LabeledAdjBiSetVertex(values.next());
      BCLabel temvertlabel=(BCLabel)temvert.getLabel(BCLabel.bcLabelPathsKey);
//      System.out.println("------------temvert:"+temvert);
//      System.out.println("-------------temvertlable :"+temvertlabel);
      
      if(temvertlabel.getStatus()==-1)
      {  
          inivert=temvert;          //initial vertex      
      }
      else                               // not the initial vertex
      { 
        if(temvert.getId().equals(key.toString()))
        {
        currentVertex=temvert;            //search for the current key  
        
        continue;
        }
      
        else
        {
//          System.out.println("precessor------------temvert:"+temvert);
          
          number+=temvertlabel.getNumber();
          distance=temvertlabel.getDsitance();
//          System.out.println("number:"+number+" distance:"+distance);
          precessorIdSet.add(temvert.getId());
//          System.out.println("precessorIDSET add :-----------------"+temvert.getId());
        
        
//        System.out.println("the predecessor is:"+temvert);
//        
        }
      }
      

    }
    

    
  
    if(inivert!=null)             //process the init vertex
    {
      BCLabel inilabel=(BCLabel)currentVertex.getLabel(BCLabel.bcLabelPathsKey);   //the init vert is just a signal, the data is in the currentVertex
      inilabel.setStatus(1);
      inilabel.setDistance(0);
      inilabel.setNumber(1);
      currentVertex.setLabel(BCLabel.bcLabelPathsKey, inilabel);
      outputKey.set(currentVertex.getId());
      output.collect(outputKey, currentVertex);
      Set<AdjVertexEdge> forward_vertex=currentVertex.getForwardVertexes();
      String to;
      for(AdjVertexEdge item:forward_vertex)
      {
        to=item.getOpposite();
        if(!changeflag){
          recordContinue();
        }
        outputKey.set(to);
        output.collect(outputKey, currentVertex);
//        System.out.println("in the process of initial node the output--------outputKey"+outputKey+
//            "   currentvertex    :"+currentVertex);
        
      }
      
    }
    else                          //process the normal node
    {  
      BCLabel curlabel=(BCLabel)currentVertex.getLabel(BCLabel.bcLabelPathsKey);
      if(curlabel.getDsitance()==-1)      // the node has not been visited
      {  
//      System.out.println("the size of IDSET:"+precessorIdSet.size());
        if(precessorIdSet.size()!=0)
        {  

        distance+=1;
        curlabel.setDistance(distance);
//        System.out.println("--------------now the curlabel distance is :"+curlabel.getDsitance());
        
        for(String item:precessorIdSet)
        {
          curlabel.addPrecessor(item); //these vertexes are the precessor of the current vertex

        }
        curlabel.setNumber(number);
        curlabel.setStatus(1);
        outputKey.set(currentVertex.getId());
        output.collect(outputKey, currentVertex);
        
              Set<AdjVertexEdge> forward_vertex=currentVertex.getForwardVertexes();
        
        Set<String> rest=new HashSet<String>();
        for(AdjVertexEdge item:forward_vertex)
        {
          String to=item.getOpposite();
//          System.out.println("-----------the to is:"+to);
          if(!precessorIdSet.contains(to))
          {
            rest.add(to);
          }
          
        }
        
        Iterator<String> itr=rest.iterator();
        while(itr.hasNext())
        {
          if(!changeflag){
            recordContinue();
          }
          outputKey.set(itr.next());
          output.collect(outputKey, currentVertex);
//          System.out.println("in the process of normal node the output--------outputKey:"+outputKey+
//              "   currentvertex    :"+currentVertex);
        }
        }
        else                        // there is only one node and the node is the current node
        {
        outputKey.set(key.toString());
        output.collect(outputKey, currentVertex);
//        System.out.println("in the process of normal node(only one) the output--------outputKey"+outputKey+
//            "   currentvertex    :"+currentVertex);
        }
        
      }
      else                             //the node has been visited
      {
        outputKey.set(currentVertex.getId());
        output.collect(outputKey, currentVertex);
//        
//        System.out.println("processe visited node the output--------outputKey"+outputKey+
//            "   currentvertex    :"+currentVertex);
      }
//      System.out.println("here forward reducer ends-----------------------------------");
      
      }
    
    }catch(IOException e)
    {
      e.printStackTrace();
    }
    
  }
  
  

  public void configure(JobConf job) {
    super.configure(job);
    this.job=job;
  }
  
  private void recordContinue() throws IOException            // to indicate whether there still needs another mapreduce step 
  {
    if(changeflag)
    {
      return;
    }

    changeflag=true;

    String continueFile=job.get(BCForwardStep.continueFileKey);

    if(continueFile!=null)
    {
      FileSystem fs=FileSystem.get(job);
      fs.mkdirs(new Path(continueFile));
      fs.close();
    }
  }
}

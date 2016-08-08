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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.pagerank.normal.PageRankAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


public class PageRankReducer extends GraphAlgorithmMapReduceBase 
    implements Reducer<Text, ObjectWritable, Text, LabeledAdjSetVertex> {
  
  JobConf job=null;
  boolean changeFlag=false;
  
  private double stopThreshold=0.01;
  private double dampingFactor=1;
  private String continueFile;

  @Override
  public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
      throws IOException {
    LabeledAdjSetVertex vertex=null;
    double newPR=0;
    
    while (values.hasNext()) {
      ObjectWritable obj=values.next();
      
      if(obj.get() instanceof PageRankLabel) {
        newPR+=((PageRankLabel)obj.get()).getPr();
        continue;
      }
      
      vertex=(LabeledAdjSetVertex) obj.get();
    }
    
    if(vertex!=null) {      
      PageRankLabel label=(PageRankLabel) vertex.getLabel(PageRankLabel.pageRankLabelKey);
      
      if(label.isReachable()) {      
        newPR= dampingFactor*newPR + (1-dampingFactor)*label.getInitWeight();
        
        label.setPr( newPR );
        vertex.setLabel(PageRankLabel.pageRankLabelKey, label);
        
        if( Math.abs(label.getPrepPR()-label.getPr()) > stopThreshold) {
          recordContinue();
        }
      }

      output.collect(key, vertex);
    }
  }

  public void configure(JobConf job) {
    super.configure(job);
    this.job=job;
    
    String property=context.getParameter(PageRankAlgorithm.pageRankDampingFactorKey);
    if(property!=null) {
      dampingFactor=Double.valueOf(property);
    }
    
    property=context.getParameter(PageRankAlgorithm.pageRankStopThresholdKey);
    if(property!=null) {
      stopThreshold=Double.valueOf(property);
    }
    
//    property=context.getProperties().getProperty(PageRankAlgorithm.pageRankVertexNumberKey);
//    if(property!=null) {
//      vertexNumber=Long.valueOf(property);
//    }
    
    continueFile=context.getParameter(PageRankStep.continueFileKey);
    if(continueFile==null) {
      continueFile="continue";
    }
  }
  
  private void recordContinue() throws IOException {
    if(changeFlag) {
      return;
    }
    
    changeFlag=true;
          
    if(continueFile!=null) {  // create the directory.
      FileSystem client=FileSystem.get(job);
      client.mkdirs(new Path(continueFile));
    }
  }
}

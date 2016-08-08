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

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.pagerank.normal.PageRankAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * According to pagerank's random walk perspective, next step from a node with no outbound edge would jump to a random node.
 * So the rank in such node should collected and distributed to every node.
 * This Mapper will add such rank to every node.
 * @author Cai Bin
 */
public class PageRankCorrectionMapper extends GraphAlgorithmMapReduceBase
    implements Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex> {
  double correctionValue=0.0;

  JobConf job=null;
  private String continueFile;
  boolean changeFlag=false;
  private double stopThreshold=0.01;

  /**
   * Very simple Mapper. Add the correction value to every node's PR value.
   * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void map(Text key, LabeledAdjSetVertex value,
      OutputCollector<Text, LabeledAdjSetVertex> collector, Reporter reporter)
      throws IOException {
    PageRankLabel label=(PageRankLabel) value.getLabel(PageRankLabel.pageRankLabelKey);
    if(label==null) {
      label=new PageRankLabel();
      label.setInitVertex(false);
      label.setReachable(false);
      label.setPr(0);
      label.setPrepPR(0);
    } 

    if(label.isReachable()) {
      System.out.println("Vertex ID: "+value.getId()+"  correctionValue: "+correctionValue);
      System.out.println("Vertex ID: "+value.getId()+"  label.getPrepPR(): "+label.getPrepPR());
      
      label.setPr((label.getPr()+correctionValue*label.getInitWeight()));
    }
    
    if( Math.abs(label.getPrepPR()-label.getPr()) > stopThreshold) {
      recordContinue();
    }
    
    // emit the vertex
    value.setLabel(PageRankLabel.pageRankLabelKey, label);
    collector.collect(key, value);
  }

  /**
   * To get the correction value.
   * @see org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    super.configure(job);
    this.job=job;
    
    String property=context.getParameter(PageRankAlgorithm.pageRankVertexAdjKey);
    if(property!=null) {
      correctionValue=Double.valueOf(property);
    }
    
    property=context.getParameter(PageRankAlgorithm.pageRankStopThresholdKey);
    if(property!=null) {
      stopThreshold=Double.valueOf(property);
    }

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
          
    if(continueFile!=null) {
      DFSClient client=new DFSClient(job);
      client.mkdirs(continueFile);
      client.close();
    }
  }
}

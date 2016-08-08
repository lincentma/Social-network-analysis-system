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
import java.net.InetSocketAddress;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;



/**
 * Mapper of PageRank algorithm.
 * This mapper will divided the score of the vertex by the number of out link and then pass the quotient to destinations
 * @author Cai Bin
 */
public class PageRankMapper extends GraphAlgorithmMapReduceBase
    implements Mapper<Text, LabeledAdjSetVertex, Text, ObjectWritable> {
  JobConf jobConf=null;
  
  /**
   * Output value.
   * Since we have two outputs: LabeledAdjSetVertex for vertex and PageRankLabel for quotient, 
   * wrapper ObjectWritable should be used here.
   */
  ObjectWritable outputValue=new ObjectWritable(); 
  Text dest=new Text();
  
  PageRankLabel label=new PageRankLabel();
  
  private long zeroOutDegreeVertexCount=0L;
  private double zeroOutDegreeVertexRank=0.0;  
  private String taskID=null;   // TaskID to send 
  
  @Override
  public void map(Text key, LabeledAdjSetVertex value,
      OutputCollector<Text, ObjectWritable> collector, Reporter reporter)
      throws IOException {    
    PageRankLabel label=(PageRankLabel) value.getLabel(PageRankLabel.pageRankLabelKey);
    if(label==null) {
      label=new PageRankLabel();
      label.setInitVertex(false);
      label.setReachable(false);
      label.setPr(0);
      label.setPrepPR(0);
      label.setInitWeight(0);
    }
    
    label.setPrepPR(label.getPr());  //set previous page rank value.
    
    // emit the vertex
    value.setLabel(PageRankLabel.pageRankLabelKey, label);
    outputValue.set(value);
    collector.collect(key, outputValue);

    if(! label.isReachable()) {
      return;
    }
    
    // emit PR for neighbors
    if(value.getOpposites().size()>0) {
        double avgPR=label.getPr()/value.getOpposites().size();
        
        label.setPr(avgPR);
        outputValue.set(label);
        
        for(AdjVertexEdge edge: value.getOpposites()) {
          dest.set(edge.getOpposite());
        collector.collect(dest, outputValue);
        }
    } else {  // collect score
      zeroOutDegreeVertexCount++;
      zeroOutDegreeVertexRank+=label.getPr();
    }
  }

  /** 
   * Keep JobConf to read parameters and create communication to PageRankStep.
   * @see org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    super.configure(job);
    jobConf=job;
        
    TaskAttemptID taskId = TaskAttemptID.forName(job.get("mapred.task.id"));
    taskID=taskId.getTaskID().toString();
  }
  
  /**
   * Report possible score by vertex which is without out link.
   * @see org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase#close()
   */
  public void close() throws IOException {
    if(zeroOutDegreeVertexCount>0) {
      ZeroOutDegreeVertexRankCollector rankCollector=null;
      String hostName=context.getParameter(PageRankStep.rankCollectorHost);
      int port=Integer.valueOf(context.getParameter(PageRankStep.rankCollectorPort));
      
      InetSocketAddress addr=new InetSocketAddress(hostName, port);
      rankCollector=(ZeroOutDegreeVertexRankCollector)RPC.getProxy(ZeroOutDegreeVertexRankCollector.class, 
          PageRankStep.protocolVersion, addr, jobConf);
      rankCollector.postRank(taskID, zeroOutDegreeVertexCount, zeroOutDegreeVertexRank);
      RPC.stopProxy(rankCollector);
    }
  }
}

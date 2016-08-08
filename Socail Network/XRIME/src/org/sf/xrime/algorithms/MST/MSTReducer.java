/*
 * Copyright (C) yangyin@BUPT. 2009.
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
package org.sf.xrime.algorithms.MST;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTEdgeStatesLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTMessageAcceptLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTMessageChangeRootLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTMessageConnectLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTMessageInitiateLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTMessageRejectLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTMessageReportLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTMessageTestLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTMessageTimestampLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTTimestampTool;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTVertexInforLabel;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTWEdgesLabel;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * This class is used to compare two MST messages by their timestamp
 * @author YangYin
 */

class MSTMessageComparator implements Comparator<Object>
{
  public final int compare(Object msg1, Object msg2)
  {
    String timeStamp1 = ((MSTMessageTimestampLabel)((LabeledAdjVertex)(msg1)).getLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel)).getTimeStamp();
    String timeStamp2 = ((MSTMessageTimestampLabel)((LabeledAdjVertex)(msg2)).getLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel)).getTimeStamp();
    return timeStamp1.compareTo(timeStamp2);
  }
}

/**
 * The reducer class for the computing part of MST algorithm
 * Most of the message processing job is done here
 * @author YangYin
 * @see org.apache.hadoop.mapred.Mapper
 */
public class MSTReducer extends GraphAlgorithmMapReduceBase 
    implements Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  
  JobConf job = null;
  boolean changeFlag = false;
  Text outputKey = new Text();  

  
  public void configure(JobConf job) {
    super.configure(job);
    this.job=job;
  }
  
  /**
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void reduce(Text key, Iterator<LabeledAdjVertex> values,
      OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
      throws IOException {
    LabeledAdjVertex  currentVertex = null;
    List<LabeledAdjVertex> messageQueue = new LinkedList<LabeledAdjVertex>();
    
    /**
     * Collect the current vertex and all the messages sent to it
     */
    while (values.hasNext()) {
      LabeledAdjVertex vertex=new LabeledAdjVertex(values.next());
      if(key.toString().equals(vertex.getId()))
        currentVertex = vertex;
      else
        messageQueue.add(vertex);
    }
    /**
     * Sort all the message according to their time stamp
     */
    Collections.sort(messageQueue, new MSTMessageComparator());
    
    /**
     * Process all the messages
     */
    processAllMessages(key, currentVertex, output, messageQueue);
    output.collect(key, currentVertex);
  }
  public void processAllMessages(Text key, LabeledAdjVertex currentVertex, 
      OutputCollector<Text, LabeledAdjVertex> output, List<LabeledAdjVertex> messageQueue)
      throws IOException
  {
    Iterator<LabeledAdjVertex> queueIter = messageQueue.iterator();
    while(queueIter.hasNext())  {
      LabeledAdjVertex mstMessage = (LabeledAdjVertex)queueIter.next();
    
      if(mstMessage != null) {
        MSTVertexInforLabel mstVertexInforLabel = (MSTVertexInforLabel)currentVertex.getLabel(MSTVertexInforLabel.mstVertexInforLabel);
        MSTWEdgesLabel mstWEdgeLabel = (MSTWEdgesLabel)currentVertex.getLabel(MSTWEdgesLabel.mstWEdgesLabel);
        MSTEdgeStatesLabel mstEdgeStatesLabel = (MSTEdgeStatesLabel)currentVertex.getLabel(MSTEdgeStatesLabel.mstEdgeStatesLabel);
        /**
         * The message is "Connect"
         */
        if(mstMessage.getLabel(MSTMessageConnectLabel.mstMessageConnectLabel) != null)
        {
          MSTMessageConnectLabel mstMessageConnectLabel = (MSTMessageConnectLabel)mstMessage.getLabel(MSTMessageConnectLabel.mstMessageConnectLabel);
          /**
           * check if this message should be put to the message queue(re-emit)
           */
          if(MSTTool.onRecvConnectMsg(key.toString(), mstVertexInforLabel, mstWEdgeLabel, mstEdgeStatesLabel, output, mstMessage.getId(), mstMessageConnectLabel.getConnectLevel()) == 1)
          {
            /**
             * refresh time-stamp
             */
            MSTMessageTimestampLabel timeStampLabel = 
              (MSTMessageTimestampLabel)mstMessage.getLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel);
            timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
            output.collect(key, mstMessage);
          }
        }
        
        /**
         *  The message is "Initiate"
         */
        else if(mstMessage.getLabel(MSTMessageInitiateLabel.mstMessageInitiateLabel) != null)
        {
          /**
           * call the onRecvInitialMsg procedure
           */
          MSTMessageInitiateLabel mstMessageInitiateLabel = (MSTMessageInitiateLabel)mstMessage.getLabel(MSTMessageInitiateLabel.mstMessageInitiateLabel);
          MSTTool.onRecvInitialMsg(key.toString(), mstVertexInforLabel, mstWEdgeLabel, mstEdgeStatesLabel, output, mstMessage.getId(), mstMessageInitiateLabel);
        }
        
        /** 
         * The message is "Test"
         */
        else if(mstMessage.getLabel(MSTMessageTestLabel.mstMessageTestLabel) != null)
        {
          /**
           * Call the onRecvTestMsg procedure
           */
          MSTMessageTestLabel mstMessageTestLabel = (MSTMessageTestLabel)mstMessage.getLabel(MSTMessageTestLabel.mstMessageTestLabel);
          /**
           * Check if the message should be put to the message queue(re-emit)
           */
          if(MSTTool.onRecvTestMsg(key.toString(), mstVertexInforLabel, mstWEdgeLabel, mstEdgeStatesLabel, output, mstMessage.getId(), mstMessageTestLabel.getFragLevel(), mstMessageTestLabel.getFragIdentity()) == 1)
          {
            /**
             * refresh time-stamp
             */
            MSTMessageTimestampLabel timeStampLabel = 
              (MSTMessageTimestampLabel)mstMessage.getLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel);
            timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
            output.collect(key, mstMessage);
          }
        }
        
        /** 
         * The message is "Accept"
         */
        else if(mstMessage.getLabel(MSTMessageAcceptLabel.mstMessageAcceptLabel) != null)
        {
          /**
           * call the onRecvAcceptMsg procedure
           */
          MSTTool.onRecvAcceptMsg(key.toString(), mstVertexInforLabel, mstWEdgeLabel, mstEdgeStatesLabel, output, mstMessage.getId());
        }
        
        /** 
         * The message is "Reject"
         */
        else if(mstMessage.getLabel(MSTMessageRejectLabel.mstMessageRejectLabel) != null)
        {
          /**
           * call the onRecvRejectMsg procedure
           */
          MSTTool.onRecvRejectMsg(key.toString(), mstVertexInforLabel, mstWEdgeLabel, mstEdgeStatesLabel, output, mstMessage.getId());
        }
        
        /**
         * The message is "Report"
         */
        else if(mstMessage.getLabel(MSTMessageReportLabel.mstMessageReportLabel) != null)
        {
          MSTMessageReportLabel mstMessageReportLabel = (MSTMessageReportLabel)mstMessage.getLabel(MSTMessageReportLabel.mstMessageReportLabel);
          /**
           * check if the message should be re-emitted
           */
          int returnType = MSTTool.onRecvReportMsg(key.toString(), mstVertexInforLabel, mstWEdgeLabel, mstEdgeStatesLabel, output, mstMessage.getId(), mstMessageReportLabel.getBestWeight(), mstMessageReportLabel.getEdgeIdentity());
          if(returnType == 1)
          {
            /**
             * refresh time-stamp
             */
            MSTMessageTimestampLabel timeStampLabel = 
              (MSTMessageTimestampLabel)mstMessage.getLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel);
            timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
            output.collect(key, mstMessage);
          }
          /**
           * job termination
           */
          else if(returnType == -1)
          {
            /**
             * create the terminate directory to stop the MST algorithm
             */
            String terminateFile = context.getParameter(MSTStep.terminateFileKey);
            if(terminateFile != null) {
              FileSystem client=FileSystem.get(job);
              client.mkdirs(new Path(terminateFile));
            }
          }
        }
        /**
         *  The message is "Change Root"
         */
        else if(mstMessage.getLabel(MSTMessageChangeRootLabel.mstMessageChangeRootLabel) != null)
        {
          MSTTool.onRecvChangeRootMsg(key.toString(), mstVertexInforLabel, mstWEdgeLabel, mstEdgeStatesLabel, output, mstMessage.getId());
        }
      }
    }
  }
}

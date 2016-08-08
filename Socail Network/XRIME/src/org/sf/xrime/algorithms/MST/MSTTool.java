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
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.sf.xrime.algorithms.MST.MSTLabel.MSTEdgeStateVariable;
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
import org.sf.xrime.model.edge.WeightOfEdge;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * This class defines the main actions and procedures that should be taken 
 * during the MST algorithm, so it can be seen as the logical processing part
 * @author YangYin
 *
 */
public class MSTTool
{
  /**
   * Wake Up procedure. Initially all the vertexes are in 'Sleep' state
   * vertexes with 'true' value for autoWakeUp will wake up automatically
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgeLabel
   * @param collector
   * @throws IOException
   */
  public static void wakeUp(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel,
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector) 
      throws IOException 
  {
    Set<String> keySet = wEdgesLabel.getWEdges().getLabels().keySet();
    Iterator<String> iter = keySet.iterator();
    WeightOfEdge minWeight = new WeightOfEdge(Integer.MAX_VALUE);
    Text resVertex = new Text();

    while(iter.hasNext())
    {
      String curEdgeNode = (String)iter.next();
      int state = edgeStatesLabel.getEdgeState(curEdgeNode).getState();
      WeightOfEdge weight = wEdgesLabel.getWEdge(curEdgeNode);
      /**
       * The edge's state is "base"
       */
      if(state == 1)
      {
        /**
         * firstly, check if it's a smaller weight, 
         * or equal weight but smaller fragIdentity
         */
        if(weight.getWeight() < minWeight.getWeight() || 
            (weight.getWeight() == minWeight.getWeight() && curEdgeNode.compareTo(resVertex.toString()) < 0))
        {
          resVertex.set(curEdgeNode);
          minWeight.setWeight(weight.getWeight());
        } 
      }
    }
    /**
     * Change status
     */
    //Set the edge to be branch
    edgeStatesLabel.setEdgeState(resVertex.toString(), new MSTEdgeStateVariable(-1));
    //Set node status to be "Found"
    inforLabel.setStatus(1);
    //Set find count to be 0
    inforLabel.setFindCount(0);
    //Set fragLevel to be 0
    inforLabel.setFragLevel(0);
    //Create connect label
    MSTMessageConnectLabel conLabel = new MSTMessageConnectLabel();
    //Set connectLevel parameter to be 0
    conLabel.setConnectLevel(0);
    /**
     * Create connect message
     */
    //Create time stamp label and set time stamp
    MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
    timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
    LabeledAdjVertex connectMsg = new LabeledAdjVertex();
    //Create connect message and set related labels
    //set sender to be the current vertex
    connectMsg.setId(currentVertexId);
    connectMsg.setLabel(MSTMessageConnectLabel.mstMessageConnectLabel, conLabel);
    connectMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
    /**
     * Emit the new Connect Message, receiver is resVertex
     */
    collector.collect(resVertex, connectMsg);
    return;
  }
  
  /**
   * This procedure is called when a vertex receives a "Connect" message from other vertex
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgeLabel
   * @param collector
   * @param fromVertexId
   * @param fromLevel
   * @return 0: normal return, 1: re-emit the connect message
   * @throws IOException
   */
  public static int onRecvConnectMsg(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector, String fromVertexId, int fromLevel)
        throws IOException
  {
    /**
     * The vertex will wake up automatically if it is still in 'Sleep' state
     */
    if(inforLabel.getStatus() == -1)
      MSTTool.wakeUp(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    
    /**
     *  If fromLevle < current fragment level, the request is acceptable
     */
    if(fromLevel < inforLabel.getFragLevel())
    {
      /**
       * Create initial message
       */
      //set the edge state to be 'Branch'
      edgeStatesLabel.setEdgeState(fromVertexId, new MSTEdgeStateVariable(-1));
      //send initiate message on edge fromVertexId
      Text recVertex = new Text(fromVertexId);
      LabeledAdjVertex initiateMsg = new LabeledAdjVertex();
      initiateMsg.setId(currentVertexId);
      //Create initial label
      MSTMessageInitiateLabel initiateLabel= new MSTMessageInitiateLabel();
      initiateLabel.setFragIdentity(inforLabel.getFragIdentity());
      initiateLabel.setFragLevel(inforLabel.getFragLevel());
      initiateLabel.setState(inforLabel.getStatus());
      //Create time stamp label
      MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
      timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
      //Add related labels for initial message
      initiateMsg.setLabel(MSTMessageInitiateLabel.mstMessageInitiateLabel, initiateLabel);
      initiateMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
      collector.collect(recVertex, initiateMsg);
      
      /**
       * If current vertex state is 'Find' then find count should plus 1
       */
      if(inforLabel.getStatus() == 0)
      {
        int curFindCount = inforLabel.getFindCount();
        inforLabel.setFindCount(curFindCount + 1);
      }
    }
    else 
    {
      /**
       * Check whether edge state = 'basic'
       */
      if(edgeStatesLabel.getEdgeState(fromVertexId).getState() == 1)
      {
        //re-emit the connect message
        return 1;
      }
      else
      {
        /**
         * Create initial message
         */
        //send initiate message on edge fromVertexId
        Text recVertex = new Text(fromVertexId);
        LabeledAdjVertex initiateMsg = new LabeledAdjVertex();
        initiateMsg.setId(currentVertexId);
        //Create initial label*********/
        MSTMessageInitiateLabel initiateLabel= new MSTMessageInitiateLabel();
        //Set fragment identity to be the new edge which is created by combing the two vertexes' id
        initiateLabel.setFragIdentity(getIdentity(currentVertexId, fromVertexId));
        //New level will be the original one plus 1
        int newLevel = inforLabel.getFragLevel() + 1;
        initiateLabel.setFragLevel(newLevel);
        //Status will be 'find'
        initiateLabel.setState(0);
        //Create time stamp label
        MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
        timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
        //Set related label for initial message
        initiateMsg.setLabel(MSTMessageInitiateLabel.mstMessageInitiateLabel, initiateLabel);
        initiateMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
        collector.collect(recVertex, initiateMsg);
      }
    }
    return 0;
  }
  
  /**
   * This procedure is called when a vertex receives a "Initial" message from other vertex
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgesLabel
   * @param edgeStatesLabel
   * @param collector
   * @param fromVertexId
   * @param mstMessageInitiateLabel
   * @throws IOException
   */
  public static void onRecvInitialMsg(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
        MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector, String fromVertexId, MSTMessageInitiateLabel mstMessageInitiateLabel)
          throws IOException{
    int newFragLevel = mstMessageInitiateLabel.getFragLevel();
    String newFragIdentity = mstMessageInitiateLabel.getFragIdentity();
    int newVertexState = mstMessageInitiateLabel.getState();
    
    /**
     * procedure1: update the three properties
     */
    inforLabel.setFragLevel(newFragLevel);
    inforLabel.setFragIdentity(newFragIdentity);
    inforLabel.setStatus(newVertexState);
    inforLabel.setInBranch(fromVertexId);
    inforLabel.setBestWeight(new WeightOfEdge(Integer.MAX_VALUE)); // -1 means infinite
    inforLabel.setBestEdgeIdentity("");
    inforLabel.setBestEdge("");
    /**
     * procedure2: send initiate message to each branch edge
     */
    Set<String> keySet = wEdgesLabel.getWEdges().getLabels().keySet();
    Iterator<String> iter = keySet.iterator();
    
    while(iter.hasNext())
    {
      String curEdgeVertex = (String)iter.next();
      int state = edgeStatesLabel.getEdgeState(curEdgeVertex).getState();
      //Check if the edge is a branch edge
      if(state == -1 && !curEdgeVertex.equals(fromVertexId))
      {
        //Send the initiate message to the branch edge without modifying
        Text recVertex = new Text(curEdgeVertex);
        LabeledAdjVertex initiateMsg = new LabeledAdjVertex();
        initiateMsg.setId(currentVertexId);
        MSTMessageInitiateLabel initiateLabel= (MSTMessageInitiateLabel) mstMessageInitiateLabel.clone();
        MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
        timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
        //Set related label for initial message
        initiateMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);        
        initiateMsg.setLabel(MSTMessageInitiateLabel.mstMessageInitiateLabel, initiateLabel);
        collector.collect(recVertex, initiateMsg);
        //Add find count if needed(current state in initiate message is 'Find')
        if(newVertexState == 0)
        {
          int newFindCount = inforLabel.getFindCount() + 1;
          inforLabel.setFindCount(newFindCount);
        }
      }
    }
    /**
     * procedure3: test procedure if current vertex's state is find
     */
    if(newVertexState == 0)
    {
      procedureTest(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    }
  }
  
  /**
   * Test procedure is called to find best weight and best edge
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgesLabel
   * @param edgeStatesLabel
   * @param collector
   * @throws IOException
   */
  public static void procedureTest(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector)
        throws IOException{
    //Search for the minimum weight edge whose state is basic
    Set<String> keySet = wEdgesLabel.getWEdges().getLabels().keySet();
    Iterator<String> iter = keySet.iterator();
    WeightOfEdge minWeight = new WeightOfEdge(Integer.MAX_VALUE);
    Text resVertex = new Text();

    while(iter.hasNext())
    {
      String curEdgeNode = (String)iter.next();
      int state = edgeStatesLabel.getEdgeState(curEdgeNode).getState();
      WeightOfEdge weight = wEdgesLabel.getWEdge(curEdgeNode);
      //Edge state is basic
      if(state == 1)
      {
        /**
         * If the new edge has smaller weight, or equal weight but smaller fragIdentity
         */
        if(weight.getWeight() < minWeight.getWeight() || 
            (weight.getWeight() == minWeight.getWeight() && curEdgeNode.compareTo(resVertex.toString()) < 0))
        {
          resVertex.set(curEdgeNode);
          minWeight.setWeight(weight.getWeight());
        } 
      }
    }
    /**
     * Whether there exists the target vertex
     */
    if(minWeight.getWeight() != Integer.MAX_VALUE)
    {
      //set Test edge
      inforLabel.setTestEdge(resVertex.toString());
      //emit the Test message
      LabeledAdjVertex testMsg = new LabeledAdjVertex();
      testMsg.setId(currentVertexId);
      MSTMessageTestLabel testLabel = new MSTMessageTestLabel();
      testLabel.setFragIdentity(inforLabel.getFragIdentity());
      testLabel.setFragLevel(inforLabel.getFragLevel());
      
      MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
      timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
      //Set related label for initial message
      testMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
      testMsg.setLabel(MSTMessageTestLabel.mstMessageTestLabel, testLabel);
      collector.collect(resVertex, testMsg);
    }
    /**
     * Target vertex doesn't exist
     */
    else
    {
      //Set Test Edge to be empty
      inforLabel.setTestEdge("");
      //Call report procedure
      procedureReport(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    }
  }
  
  /**
   * Procedure report is called to report the current vertex's best weight and best edge
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgesLabel
   * @param edgeStatesLabel
   * @param collector
   * @throws IOException
   */
  public static void procedureReport(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector)
        throws IOException{
    //All the Initial Messages have been replied
    if(inforLabel.getTestEdge().equals("") && inforLabel.getFindCount() == 0)
    {
      //Set current vertex state to 'Found'
      inforLabel.setStatus(1);
      //Create the new reportMsg
      LabeledAdjVertex reportMsg = new LabeledAdjVertex();
      //Set the sender to be the current vertex
      reportMsg.setId(currentVertexId);
      //Create the new report label
      MSTMessageReportLabel reportLabel = new MSTMessageReportLabel();
      reportLabel.setBestWeight(inforLabel.getBestWeight());
      MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
      timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
      
      String edgeIdentity = MSTTool.getIdentity(currentVertexId, inforLabel.getBestEdge());
      reportLabel.setEdgeIdentity(edgeIdentity);
      
      //Set related label for initial message
      reportMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
      reportMsg.setLabel(MSTMessageReportLabel.mstMessageReportLabel, reportLabel);
      //Emit the report message, the receiver(key) is the in branch vertex
      collector.collect(new Text(inforLabel.getInBranch()), reportMsg);
    }
  }
  
  /**
   * onRecvTestMsg procedure is called when a vertex receives a "Test" message
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgeLabel
   * @param collector
   * @param fromVertexId
   * @param fromLevel
   * @param fromIdentity
   * @return 0: normal return, 1: re-emit the connect message
   * @throws IOException
   */
  public static int onRecvTestMsg(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector, String fromVertexId, int fromLevel, String fromIdentity)
        throws IOException
  {
    //THe current vertex will wake up automatically if the vertex is still in 'Sleep' state
    if(inforLabel.getStatus() == -1)
      MSTTool.wakeUp(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    //if the sender's fragment level is greater than current vertex's level,the message should be re-emitted(return -1)
    if(fromLevel > inforLabel.getFragLevel())
    {
      return 1;
    }
    //If the two vertex's fragment identities are not the same then send Accept message to the sender
    if(!fromIdentity.equals(inforLabel.getFragIdentity()))
    {
      //Create the new accept message
      LabeledAdjVertex acceptMsg = new LabeledAdjVertex();
      //Set the sender to be current vertex
      acceptMsg.setId(currentVertexId);
      //Create the new accept label
      MSTMessageAcceptLabel acceptLabel = new MSTMessageAcceptLabel();
      MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
      timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
      //Set related label for initial message
      acceptMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
      acceptMsg.setLabel(MSTMessageAcceptLabel.mstMessageAcceptLabel, acceptLabel);
      //Emit the new accept message
      collector.collect(new Text(fromVertexId), acceptMsg);
    }
    else
    {
      //If the edge state is 'Basic', then set it to be 'Rejected'
      if(edgeStatesLabel.getEdgeState(fromVertexId).getState() == 1)
        edgeStatesLabel.setEdgeState(fromVertexId, new MSTEdgeStateVariable(0));
      //If the sender is no the current test edge, then send reject message to the sender
      if(!inforLabel.getTestEdge().equals(fromVertexId))
      {
        //Create the new reject message
        LabeledAdjVertex rejectMsg = new LabeledAdjVertex();
        //Set the sender to be current vertex
        rejectMsg.setId(currentVertexId);
        //Create the new reject label
        MSTMessageRejectLabel rejectLabel = new MSTMessageRejectLabel();
        MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
        timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
        //Set related label for initial message
        rejectMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
        rejectMsg.setLabel(MSTMessageRejectLabel.mstMessageRejectLabel, rejectLabel);
        //Emit the new reject message
        collector.collect(new Text(fromVertexId), rejectMsg);
      }
      //Call test procedure
      else
        procedureTest(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    }
    return 0;
  }
  
  /**
   * onRecvAcceptMsg procedure is called when a vertex receives a "Accept" message
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgesLabel
   * @param edgeStatesLabel
   * @param collector
   * @param fromVertexId
   * @throws IOException
   */
  public static void onRecvAcceptMsg(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector, String fromVertexId)
        throws IOException
  {
    //Set test edge to be empty
    inforLabel.setTestEdge("");
    //Refresh best edge and best weight
    WeightOfEdge bestWeight = inforLabel.getBestWeight();
    WeightOfEdge curWeight = wEdgesLabel.getWEdge(fromVertexId);
    //Check if bestWeigth is infinite or current weight < best weight or bestWeight = current weight, but the vertex id is smaller
    String curEdgeIdentity = MSTTool.getIdentity(currentVertexId, fromVertexId);
    if(curWeight.getWeight() < bestWeight.getWeight() 
        || (curWeight.getWeight() == bestWeight.getWeight() && curEdgeIdentity.compareTo(inforLabel.getBestEdgeIdentity()) < 0))
    {
      inforLabel.setBestEdge(fromVertexId);
      inforLabel.setBestWeight(curWeight);
      inforLabel.setBestEdgeIdentity(curEdgeIdentity);
    }
    procedureReport(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    return;
  }
  
  /**
   * The whole job's termination is checked here
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgeLabel
   * @param collector
   * @param fromVertexId
   * @param bestWeight
   * @return 0: normal return, 1: re-emit the connect message, -1: job termination
   * @throws IOException
   */
  public static int onRecvReportMsg(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector, String fromVertexId, WeightOfEdge newWeight, String edgeIdentity)
        throws IOException
  {
    //fromVertexId != in-branch
    if(!fromVertexId.equals(inforLabel.getInBranch()))
    {
      int findCount = inforLabel.getFindCount() - 1;
      inforLabel.setFindCount(findCount);
      //Refresh best weight, best edge
      WeightOfEdge bestWeight = inforLabel.getBestWeight();
      if((newWeight.getWeight() < bestWeight.getWeight()) ||
          (newWeight.getWeight() == bestWeight.getWeight() && newWeight.getWeight() != Integer.MAX_VALUE &&
              edgeIdentity.compareTo(inforLabel.getBestEdgeIdentity()) < 0))
      {
        inforLabel.setBestEdge(fromVertexId);
        inforLabel.setBestWeight(newWeight);
        inforLabel.setBestEdgeIdentity(edgeIdentity);
      }
      //Call procedureReport
      procedureReport(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    }
    //fromVertexId == in-branch
    else
    {
      //It has found its best weight and best edge(status is not 'find')
      if(inforLabel.getStatus() != 0)
      {
        //The current part has the best weight
        if((newWeight.getWeight() > inforLabel.getBestWeight().getWeight())
          || (newWeight.getWeight() == inforLabel.getBestWeight().getWeight() && newWeight.getWeight() != Integer.MAX_VALUE && 
              edgeIdentity.compareTo(inforLabel.getBestEdgeIdentity()) >= 0))
          procedureChangeRoot(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
        //The opposite part has the best weight
        else
        {
          /**
           * If the two parts' bestWeights are both infinite(-1), it means that the job should terminate, 
           * the MST has been created
           */
          if(newWeight.getWeight() == Integer.MAX_VALUE && inforLabel.getBestWeight().getWeight() == Integer.MAX_VALUE)
            return -1;
        }
      }
      //The current part is still finding its best weight, so the message should be put into queue(re-emit)
      else
        return 1;
    }
    //Normal return
    return 0;
  }
  
  /**
   * procedureChangeRoot is called to notify the change of core edge
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgesLabel
   * @param edgeStatesLabel
   * @param collector
   * @throws IOException
   */
  public static void procedureChangeRoot(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector)
        throws IOException
  {
    //Check if best edge is branch edge
    if(edgeStatesLabel.getEdgeState(inforLabel.getBestEdge()).getState() == -1)
    {
      //Create the new change root message
      LabeledAdjVertex changeRootMsg = new LabeledAdjVertex();
      //Set sender to be the current vertex
      changeRootMsg.setId(currentVertexId);
      //Create the new change root label
      MSTMessageChangeRootLabel changeRootLabel= new MSTMessageChangeRootLabel();
      MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
      timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
      //Set related label for initial message
      changeRootMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
      changeRootMsg.setLabel(MSTMessageChangeRootLabel.mstMessageChangeRootLabel, changeRootLabel);
      //Emit the new change root message, the receiver is best edge vertex
      collector.collect(new Text(inforLabel.getBestEdge()), changeRootMsg);
    }
    //Best edge is the edge of another fragment
    else
    {
      //Create the new connect message
      LabeledAdjVertex connectMsg = new LabeledAdjVertex();
      //Set the sender to be the current vertex
      connectMsg.setId(currentVertexId);
      //Create the new connect message label
      MSTMessageConnectLabel connectLabel = new MSTMessageConnectLabel();
      connectLabel.setConnectLevel(inforLabel.getFragLevel());
      MSTMessageTimestampLabel timeStampLabel = new MSTMessageTimestampLabel();
      timeStampLabel.setTimeStamp(MSTTimestampTool.getCurTimeStamp());
      //Set related label for initial message
      connectMsg.setLabel(MSTMessageTimestampLabel.mstMessageTimestampLabel, timeStampLabel);
      connectMsg.setLabel(MSTMessageConnectLabel.mstMessageConnectLabel, connectLabel);
      //Emit the new connect message, set the receiver to be best edge vertex
      collector.collect(new Text(inforLabel.getBestEdge()), connectMsg);
      //Change the status of the best edge to be branch
      edgeStatesLabel.setEdgeState(inforLabel.getBestEdge(), new MSTEdgeStateVariable(-1));
    }
    return;
  }
  
  /**
   * onRecvChangeRootMsg procedure is called when a vertex receives a "ChangeRoot" message
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgesLabel
   * @param edgeStatesLabel
   * @param collector
   * @param fromVertexId
   * @throws IOException
   */
  public static void onRecvChangeRootMsg(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector, String fromVertexId)
        throws IOException
  {
    //Just call procedureChangeRoot
    procedureChangeRoot(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    return;
  }
  
  /**
   * onRecvRejectMsg procedure is called when a vertex receives a "Reject" message
   * @param currentVertexId
   * @param inforLabel
   * @param wEdgesLabel
   * @param edgeStatesLabel
   * @param collector
   * @param fromVertexId
   * @throws IOException
   */
  public static void onRecvRejectMsg(String currentVertexId, MSTVertexInforLabel inforLabel, MSTWEdgesLabel wEdgesLabel, 
      MSTEdgeStatesLabel edgeStatesLabel, OutputCollector<Text, LabeledAdjVertex> collector, String fromVertexId)
        throws IOException
  {
    //Set the edge state to 'Rejected' if the current state is 'Basic'
    if(edgeStatesLabel.getEdgeState(fromVertexId).getState() == 1)
      edgeStatesLabel.setEdgeState(fromVertexId, new MSTEdgeStateVariable(0));
    //Call procedureTest
    procedureTest(currentVertexId, inforLabel, wEdgesLabel, edgeStatesLabel, collector);
    return;
  }
  
  /**
   * This method is called to create a identity from two vertexes'id
   * @param vertexIdA
   * @param vertexIdB
   * @return
   */
  public static String getIdentity(String vertexIdA, String vertexIdB)
  {
    String identity = "";
    if(vertexIdA.compareTo(vertexIdB) <= 0)
      identity = vertexIdA + "_" + vertexIdB;
    else
      identity = vertexIdB + "_" + vertexIdA;
    return identity;
  }
  
  /**
   * This method is called to compare two identities
   * @param fragIdentityA
   * @param fragIdentityB
   * @return
   */
  public static int compareFrageIdentity(String fragIdentityA, String fragIdentityB)
  {
    String []vertexBiA = fragIdentityA.split("_");
    String []vertexBiB = fragIdentityB.split("_");
    
    int res1;
    if((res1 = vertexBiA[0].compareTo(vertexBiB[0])) != 0)
      return res1;
    else
      return vertexBiA[1].compareTo(vertexBiB[1]);
  }
  
}

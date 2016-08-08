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
package org.sf.xrime.algorithms.partitions.connected.strongly;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
 * Propagate the id of a vertex as the label of SCC. Try to find all predecessors and
 * descendants of this vertex.
 * @author xue
 */
public class LabelPropagation extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public LabelPropagation(){
    super();
  }
  /**
   * Mapper, to do the propagation.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{
    @Override
    public void map(Text key, LabeledAdjBiSetVertex value,
        OutputCollector<Text, LabeledAdjBiSetVertex> output,
        Reporter reporter) throws IOException {
      // Get the SCC label to be propagated.
      String scc_label = this.context.getParameter(ConstantLabels.SCC_LABEL);
      
      // Check for this vertex's label forwarding status.
      if(value.getStringLabel(ConstantLabels.LABEL_FORWARD_DONE)==null){
        // This vertex hasn't propagate the SCC label to its descendants.
        if(key.toString().equals(scc_label)){
          // This is the vertex whose id is the SCC label, and it hasn't propagate
          // the label to its descendants. So, set the forward label for it.
          value.setStringLabel(ConstantLabels.FORWARD_LABEL, scc_label);
          // The do the forward propagation.
          for(AdjVertexEdge edge : value.getForwardVertexes()){
            // Prepare the notifier which is used to "propagate" the label to
            // the descendant.
            LabeledAdjBiSetVertex notifier = new LabeledAdjBiSetVertex();
            notifier.setId(edge.getOpposite());
            notifier.setStringLabel(ConstantLabels.FORWARD_LABEL, scc_label);
            output.collect(new Text(edge.getOpposite()), notifier);
          }
          // Mark this vertex as "label forward done".
          value.setStringLabel(ConstantLabels.LABEL_FORWARD_DONE, "done");
        }else{
          // Vertexes other than the one with SCC_LABEL.
          if(value.getStringLabel(ConstantLabels.FORWARD_LABEL)!=null){
            // This vertex has already been included in the forward shadow, but
            // hasn't propagate the label forward yet. So forward it.
            for(AdjVertexEdge edge : value.getForwardVertexes()){
              // Prepare the notifier which is used to "propagate" the label to
              // the descendant.
              LabeledAdjBiSetVertex notifier = new LabeledAdjBiSetVertex();
              notifier.setId(edge.getOpposite());
              // Since we only propagate a single label forward/backward a time,
              // So we could safely assume scc_label == FORWARD_LABEL.
              notifier.setStringLabel(ConstantLabels.FORWARD_LABEL, scc_label);
              output.collect(new Text(edge.getOpposite()), notifier);
            }
            // Mark this vertex as "label forward done".
            value.setStringLabel(ConstantLabels.LABEL_FORWARD_DONE, "done");
          }else{
            // This vertex does not have any label to be forwarded and hasn't
            // propagate any label forward yet. So, it hasn't been included
            // in the forward shadow yet (and might never be). Do nothing here.
          }
        }
      }else{
        // This vertex has already done the label forwarding. Do nothing here.
      }
      
      // Check for this vertex's label backwarding status.
      if(value.getStringLabel(ConstantLabels.LABEL_BACKWARD_DONE)==null){
        // This vertex hasn't propagate the SCC label to its ancestors.
        if(key.toString().equals(scc_label)){
          // This is the vertex whose id is the SCC label, and it hasn't propagate
          // the label to its ancestors. So, set the backward label for it.
          value.setStringLabel(ConstantLabels.BACKWARD_LABEL, scc_label);
          // The do the backward propagation.
          for(AdjVertexEdge edge : value.getBackwardVertexes()){
            // Prepare the notifier which is used to "propagate" the label to
            // the ancestor.
            LabeledAdjBiSetVertex notifier = new LabeledAdjBiSetVertex();
            notifier.setId(edge.getOpposite());
            notifier.setStringLabel(ConstantLabels.BACKWARD_LABEL, scc_label);
            output.collect(new Text(edge.getOpposite()), notifier);
          }
          // Mark this vertex as "label forward done".
          value.setStringLabel(ConstantLabels.LABEL_BACKWARD_DONE, "done");
        }else{
          // Vertexes other than the one with SCC_LABEL.
          if(value.getStringLabel(ConstantLabels.BACKWARD_LABEL)!=null){
            // This vertex has already been included in the backward shadow, but
            // hasn't propagate the label backward yet. So backward it.
            for(AdjVertexEdge edge : value.getBackwardVertexes()){
              // Prepare the notifier which is used to "propagate" the label to
              // the ancestor.
              LabeledAdjBiSetVertex notifier = new LabeledAdjBiSetVertex();
              notifier.setId(edge.getOpposite());
              // Since we only propagate a single label forward/backward a time,
              // So we could safely assume scc_label == BACKWARD_LABEL.
              notifier.setStringLabel(ConstantLabels.BACKWARD_LABEL, scc_label);
              output.collect(new Text(edge.getOpposite()), notifier);
            }
            // Mark this vertex as "label forward done".
            value.setStringLabel(ConstantLabels.LABEL_BACKWARD_DONE, "done");
          }else{
            // This vertex does not have any label to be backwarded and hasn't
            // propagate any label backward yet. So, it hasn't been included
            // in the backward shadow yet (and might never be). Do nothing here.
          }
        }
      }else{
        // This vertex has already done the label backwarding. Do nothing here.
      }
      
      // Emit the vertex itself, whose status might be updated.
      output.collect(key, value);
    }
  }
  
  /**
   * Reducer, do settle done the forward and backward frontier of propagation.
   * @author xue
   *
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjBiSetVertex> values,
        OutputCollector<Text, LabeledAdjBiSetVertex> output, Reporter reporter)
        throws IOException {
      // Used to store the consolidated vertex.
      LabeledAdjBiSetVertex result = new LabeledAdjBiSetVertex();
      result.setId(key.toString());
      // Loop over each, copy all, no need to deep copy.
      while(values.hasNext()){
        LabeledAdjBiSetVertex curr = values.next();
        for(AdjVertexEdge edge : curr.getBackwardVertexes()){
          result.addBackwardVertex((AdjVertexEdge) edge.clone());
        }
        for(AdjVertexEdge edge : curr.getForwardVertexes()){
          result.addForwardVertex((AdjVertexEdge) edge.clone());
        }
        if(curr.getStringLabel(ConstantLabels.BACKWARD_LABEL)!=null)
          result.setStringLabel(ConstantLabels.BACKWARD_LABEL, 
              curr.getStringLabel(ConstantLabels.BACKWARD_LABEL));
        if(curr.getStringLabel(ConstantLabels.FORWARD_LABEL)!=null)
          result.setStringLabel(ConstantLabels.FORWARD_LABEL, 
              curr.getStringLabel(ConstantLabels.FORWARD_LABEL));
        if(curr.getStringLabel(ConstantLabels.LABEL_FORWARD_DONE)!=null)
          result.setStringLabel(ConstantLabels.LABEL_FORWARD_DONE, 
              curr.getStringLabel(ConstantLabels.LABEL_FORWARD_DONE));
        if(curr.getStringLabel(ConstantLabels.LABEL_BACKWARD_DONE)!=null)
          result.setStringLabel(ConstantLabels.LABEL_BACKWARD_DONE, 
              curr.getStringLabel(ConstantLabels.LABEL_BACKWARD_DONE));
      }
      output.collect(key, result);
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, LabelPropagation.class);
    conf.setJobName("LabelPropagation");
    
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are adjacent vertexes with labels (Writable)
    conf.setOutputValueClass(LabeledAdjBiSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(ReduceClass.class);
    conf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(getReducerNum());
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

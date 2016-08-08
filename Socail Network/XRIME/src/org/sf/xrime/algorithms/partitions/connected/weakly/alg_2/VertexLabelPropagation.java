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
package org.sf.xrime.algorithms.partitions.connected.weakly.alg_2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * Do the label propagation.
 * @author xue
 */
public class VertexLabelPropagation extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public VertexLabelPropagation(){
    super();
  }
  /**
   * Propagate the latest label of each vertex along the outgoing edges.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{
    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Get all outgoing edges of this vertex.
      Set<AdjVertexEdge> outgoing_vertexes = value.getOpposites();
      // Get the last label of this vertex.
      String last_label = value.getStringLabel(ConstantLabels.LAST_LABEL);
      // Get the label before last.
      String label_before_last = value.getStringLabel(ConstantLabels.LABEL_BEFORE_LAST);
      // Compare the two.
      if(last_label.compareTo(label_before_last)!=0){
        // Propagate the label only if we have label changed in the last round of iteration.
        
        // Determine whether we need to propagate label from this vertex to its neighbors.
        boolean need_propagate = true;
        for(AdjVertexEdge target_vertex : outgoing_vertexes){
          if(last_label.compareTo(target_vertex.getOpposite())>0){
            // No need to propagate.
            need_propagate = false;
            break;
          }
        }
        if(need_propagate){
          // Do the propagation.
          for(AdjVertexEdge target_vertex : outgoing_vertexes){
            // For each target vertex of this vertex.
            // Remove some apparently unnecessary cases.
            if(last_label.compareTo(target_vertex.getOpposite())<0){
              // For the target vertex, emit something.
              LabeledAdjSetVertex target = new LabeledAdjSetVertex();
              target.setId(target_vertex.getOpposite());
              // Note that, for this "new" AdjSetVertexWithLabels, the label
              // ConstantLabels.LABEL_BEFORE_LAST is null.
              target.setStringLabel(ConstantLabels.LAST_LABEL, last_label);
              // Emit it.
              output.collect(new Text(target_vertex.getOpposite()), target);
            }
          }
        }
      }
      // Emit the current info of this vertex too.
      output.collect(key, value);
    }
  }
  
  /**
   * Use a combiner to reduce the number of map output records.
   * @author weixue@cn.ibm.com
   */
  public static class CombineClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      String minimal_new_label = null;
      String last_label = null;
      while(values.hasNext()){
        LabeledAdjSetVertex curr_info = values.next();
        if(curr_info.getStringLabel(ConstantLabels.LABEL_BEFORE_LAST)!=null){
          // The last label of this vertex.
          last_label = curr_info.getStringLabel(ConstantLabels.LAST_LABEL);
          // Keep the record of label before last untouched.
          output.collect(key, curr_info);        }else{
          if(minimal_new_label == null || minimal_new_label.compareTo(
              curr_info.getStringLabel(ConstantLabels.LAST_LABEL))>0){
             minimal_new_label = curr_info.getStringLabel(ConstantLabels.LAST_LABEL);
          }
        }
      }
      
      if(minimal_new_label!=null){
        if(last_label!=null){
          if(minimal_new_label.compareTo(last_label)<0){
            // We are sure we need this minimal new label.
            LabeledAdjSetVertex target = new LabeledAdjSetVertex();
            target.setId(key.toString());
            target.setStringLabel(ConstantLabels.LAST_LABEL, minimal_new_label);
            output.collect(key, target);          }else{
            // Do nothing.
          }
        }else{
          // We don't have info about last label, so we emit the minimal new label for safety.
          LabeledAdjSetVertex target = new LabeledAdjSetVertex();
          target.setId(key.toString());
          target.setStringLabel(ConstantLabels.LAST_LABEL, minimal_new_label);
          output.collect(key, target);
        }
      }else{
        // Do nothing.
      }
    }
  }
  
  /**
   * Consolidate the latest label of this vertex, and those label info propagated from
   * adjacent vertexes, determine the new latest label of this vertex, and update other
   * info as a side effect.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{
    @Override
    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      LabeledAdjSetVertex result_vertex = null;
      // Use to select the lowest label for this vertex.
      String minimal_label = null;
      while(values.hasNext()){
        LabeledAdjSetVertex curr_info = values.next();
        // Found the one with info about outgoing edges. Use it as the base
        // to create the resulting vertex. This is a little trick.
        if(result_vertex==null && curr_info.getStringLabel(ConstantLabels.LABEL_BEFORE_LAST)!=null){
          result_vertex = new LabeledAdjSetVertex(curr_info);
          // Update the label before the last label.
          result_vertex.setStringLabel(ConstantLabels.LABEL_BEFORE_LAST, 
              result_vertex.getStringLabel(ConstantLabels.LAST_LABEL));
        }
        if(minimal_label == null || minimal_label.compareTo(
           curr_info.getStringLabel(ConstantLabels.LAST_LABEL))>0){
          minimal_label = curr_info.getStringLabel(ConstantLabels.LAST_LABEL);
        }
      }
      // Update the lowest label for this vertex.
      result_vertex.setStringLabel(ConstantLabels.LAST_LABEL, minimal_label);
      // Emit the vertex with updated labels.
      output.collect(key, result_vertex);
      if(minimal_label.compareTo(result_vertex.getStringLabel(ConstantLabels.LABEL_BEFORE_LAST))!=0)
        // Label changed.
        reporter.incrCounter("PROPAGATION", "CHANGED_LABELS", 1);
    }
  }
  
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    // Make sure there are exactly 2 parameters left.
    if (params.length != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters: " +
                         params.length + " instead of 2.");
    }

    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(new Path(params[0]));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(params[1]));
    
    setSource(src);
    setDestination(dest);
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, VertexLabelPropagation.class);
    conf.setJobName("VertexLabelPropagation");
    
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are adjacent vertexes with labels (Writable)
    conf.setOutputValueClass(LabeledAdjSetVertex.class);
    // mapper, combiner, reducer, all show up.
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(CombineClass.class);
    conf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    // Enable compression.
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(getReducerNum());
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new VertexLabelPropagation(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

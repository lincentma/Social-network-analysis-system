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
package org.sf.xrime.algorithms.partitions.connected.bi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
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
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * As the first steps of this BCC (bi-connected component) algorithm, we need to generate a
 * spanning tree of the input graph first. To this end, we need to choose the root of this
 * spanning tree first. This algorithm is used to choose the root. Currently, we choose the
 * vertex with largest degree as the root, which is used as a heuristics to minimize the depth
 * of the resulting spanning tree.
 * @author xue
 */
public class SpanningTreeRootChoose extends GraphAlgorithm {
  /**
   * A label name.
   */
  public static final String IN_OUT_DEGREE = "in_out_degree";
  /**
   * The key used to accumulate map output.
   */
  public static final String SPANNING_TREE_ROOT = "spanning_tree_root";
  /**
   * Default constructor.
   */
  public SpanningTreeRootChoose(){
    super();
  }
  /**
   * Emit the degree of each vertex.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex> {
    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      int in_out_degree = (value.getOpposites() == null) ? 0 : value.getOpposites().size();
      // Generate the output value.
      LabeledAdjSetVertex result = new LabeledAdjSetVertex();
      result.setId(key.toString());
      result.setLabel(IN_OUT_DEGREE, new IntWritable(in_out_degree));
      output.collect(new Text(SPANNING_TREE_ROOT), result);
    }
  }
  
  /**
   * Emit the id of the vertex with largest in + out degree.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, Text>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      int largest_value = 0;
      String root_vertex_id = null;
      // Loop over all degree values and pick the largest one.
      while(values.hasNext()){
        LabeledAdjSetVertex curr = values.next();
        int curr_val = ((IntWritable)(curr.getLabel(IN_OUT_DEGREE))).get();
        if(curr_val > largest_value){
          largest_value = curr_val;
          root_vertex_id = curr.getId();
        }
      }
      // Found the pivot vertex.
      output.collect(new Text(SPANNING_TREE_ROOT), new Text(root_vertex_id));
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, SpanningTreeRootChoose.class);
    conf.setJobName("SpanningTreeRootChoose");
    
    // This is necessary.
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(LabeledAdjSetVertex.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(MapClass.class);        
    // Since k2,v2 is different from k3,v3. No combiner is permitted.
    conf.setReducerClass(ReduceClass.class);
    // The format of input data is generated with WritableSerialization.
    conf.setInputFormat(SequenceFileInputFormat.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    // Only one reducer is permitted, or the largest value will be wrong.
    conf.setNumReduceTasks(1);
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

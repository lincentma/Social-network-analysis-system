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
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
 * This algorithm is used to choose the pivot vertex for label propagation. The criteria
 * is to choose the vertex with largest incoming+outgoing degree.
 * @author xue
 */
public class PivotChoose extends GraphAlgorithm {
  /**
   * A label name.
   */
  public static final String IN_OUT_DEGREE = "in_out_degree";
  /**
   * The key used to accumulate map output.
   */
  public static final String KEY_PIVOT = "pivot_vertex";
  /**
   * Default constructor.
   */
  public PivotChoose(){
    super();
  }
  /**
   * Emit vertex id and incoming+outgoing degree as value. The incoming+outgoing degree
   * is specified as a label value.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{

    @Override
    public void map(Text key, LabeledAdjBiSetVertex value,
        OutputCollector<Text, LabeledAdjBiSetVertex> output, Reporter reporter)
        throws IOException {
      int in_degree = (value.getBackwardVertexes()==null) ? 0 : value.getBackwardVertexes().size();
      int out_degree = (value.getForwardVertexes()==null) ? 0 : value.getForwardVertexes().size();
      // Generate the output value.
      LabeledAdjBiSetVertex result = new LabeledAdjBiSetVertex();
      result.setId(key.toString());
      result.setLabel(IN_OUT_DEGREE, new IntWritable(in_degree+out_degree));
      output.collect(new Text(KEY_PIVOT), result);
    }
  }
  /**
   * Emit the vertex with largest incoming+outgoing degree.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjBiSetVertex, Text, Text>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjBiSetVertex> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      int largest_value = 0;
      String pivot_key = null;
      // Loop over all degree values and pick the largest one.
      while(values.hasNext()){
        LabeledAdjBiSetVertex curr = values.next();
        int curr_val = ((IntWritable)(curr.getLabel(IN_OUT_DEGREE))).get();
        if(curr_val > largest_value){
          largest_value = curr_val;
          pivot_key = curr.getId();
        }
      }
      // Found the pivot vertex.
      output.collect(new Text(KEY_PIVOT), new Text(pivot_key));
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, PivotChoose.class);
    conf.setJobName("PivotChoose");
    
    // This is necessary.
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(LabeledAdjBiSetVertex.class);
    // the keys are a pseudo one.
    conf.setOutputKeyClass(Text.class);
    // the values are chosen vertex id.
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

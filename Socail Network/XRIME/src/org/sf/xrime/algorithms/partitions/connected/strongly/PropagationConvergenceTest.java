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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
 * This is used to check for convergence of label propagation. We count the number of forward and
 * backward labels settled down. If this number does not change, the label propagation converges.
 * @author xue
 *
 */
public class PropagationConvergenceTest extends GraphAlgorithm {
  /**
   * Key used to accumulate propagated label number.
   */
  public static final String KEY_LABEL_NUM = "key_label_num";
  /**
   * Default constructor.
   */
  public PropagationConvergenceTest(){
    super();
  }
  /**
   * Mapper. Check the forward/backward label number of each vertex. (Please recall that
   * we only propagate a single label at a time.) No reducer is needed. We only emit
   * key-value pairs when the label exists, as a result, the map output records number
   * is the number we want. We eliminate the overhead of transfering map output to reducer,
   * and the overhead of reading from reduce output HDFS file.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjBiSetVertex, Text, IntWritable>{

    @Override
    public void map(Text key, LabeledAdjBiSetVertex value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      if(value.getStringLabel(ConstantLabels.BACKWARD_LABEL)!=null)
        output.collect(new Text(KEY_LABEL_NUM), new IntWritable(1));
      if(value.getStringLabel(ConstantLabels.FORWARD_LABEL)!=null)
        output.collect(new Text(KEY_LABEL_NUM), new IntWritable(1));
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, PropagationConvergenceTest.class);
    conf.setJobName("PropagationConvergenceTest");
    
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are integers (Writable)
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(MapClass.class);        
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(0);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

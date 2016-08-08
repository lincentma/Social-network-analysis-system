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
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * This algorithm is used to check for convergence of the spanning tree 
 * construction prcess.
 * @author xue
 *
 */
public class SpanningConvergenceTest extends GraphAlgorithm {
  /**
   * Used as k2.
   */
  private static final Text VERTEXES_OUT_OF_TREE = new Text("out_of_tree");
  /**
   * Used as v2.
   */
  private static final IntWritable ONE = new IntWritable(1);
  /**
   * Default constructor.
   */
  public SpanningConvergenceTest(){
    super();
  }
  /**
   * Check for convergence by counting the number of vertexes which haven't got
   * their PATH_TO_ROOT set.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, IntWritable>{
    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      if(value.getLabel(ConstantLabels.PATH_TO_ROOT)==null){
        // Count those vertexes who haven't got their PATH_TO_ROOT set.
        output.collect(VERTEXES_OUT_OF_TREE, ONE);
      }
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, SpanningConvergenceTest.class);
    conf.setJobName("SpanningConvergenceTest");
    
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

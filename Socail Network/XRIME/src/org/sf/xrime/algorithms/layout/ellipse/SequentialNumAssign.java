/*
 * Copyright (C) liuchangyan@BUPT. 2009.
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
package org.sf.xrime.algorithms.layout.ellipse;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
import org.sf.xrime.algorithms.layout.ConstantLabels;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * This program assign a sequential numbers to each vertex. 
 * @author liu chang yan
 */
public class SequentialNumAssign extends GraphAlgorithm {
  /**
   * Used to assign sequence numbers.
   */
  private static final String THE_KEY = "the_key";
  /**
   * Default constructor.
   */
  public SequentialNumAssign(){
    super();
  }
  
  /**
   * Mapper.
   * @author liu chang yan
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, AdjSetVertex, Text, LabeledAdjSetVertex>{
    @Override
    public void map(Text key, AdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      output.collect(new Text(THE_KEY), new LabeledAdjSetVertex(value));
    }
  }
  
  /**
   * Reducer. Single key is used to accumulate all vertexes in order to give
   * sequential numbers to them.
   * @author liu chang yan
   *
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      int sequential_num = 0;
      while(values.hasNext()){
        LabeledAdjSetVertex curr_vertex = values.next();
        curr_vertex.setLabel(ConstantLabels.SEQUENTIAL_NUM, new IntWritable(sequential_num));
        output.collect(new Text(curr_vertex.getId()), curr_vertex);
        sequential_num++;
      }
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, SequentialNumAssign.class);
    conf.setJobName("SequentialNumAssign");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LabeledAdjSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner or reducer is needed.
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
    // The numbers here do not matter.
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(getReducerNum());
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}
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
package org.sf.xrime.algorithms.layout.gfr;

import java.io.IOException;
import java.util.Random;

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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.layout.ConstantLabels;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * Generate random initial layout for the graph.
 * @author xue
 */
public class RandomInitialLayoutGenerate extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public RandomInitialLayoutGenerate(){
    super();
  }
  
  /**
   * Mapper is enough to generate the initial layout.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, AdjSetVertex, Text, LabeledAdjSetVertex>{
    /**
     * Class level random number generator.
     */
    public static Random rn_generator = new Random(System.currentTimeMillis());
    
    @Override
    public void map(Text key, AdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Get the specified size of the display frame.
      int max_x = Integer.parseInt(context.getParameter(ConstantLabels.MAX_X_COORDINATE));
      int max_y = Integer.parseInt(context.getParameter(ConstantLabels.MAX_Y_COORDINATE));
      // Generate random x, y coordinates.
      int x = rn_generator.nextInt(max_x);
      int y = rn_generator.nextInt(max_y);
      // Set the coordinates as label values.
      LabeledAdjSetVertex result = new LabeledAdjSetVertex(value);
      result.setLabel(ConstantLabels.X_COORDINATE, new IntWritable(x));
      result.setLabel(ConstantLabels.Y_COORDINATE, new IntWritable(y));
      output.collect(key, result);
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, RandomInitialLayoutGenerate.class);
    conf.setJobName("RandomInitialLayoutGenerate");
 
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertexes (Writable)
    conf.setOutputValueClass(LabeledAdjSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner or reducer is needed.
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    // Make sure the random numbers are good.
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

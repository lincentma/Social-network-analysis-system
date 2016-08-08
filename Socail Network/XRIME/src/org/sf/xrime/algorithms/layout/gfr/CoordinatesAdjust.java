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

import org.apache.hadoop.io.DoubleWritable;
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
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * This algorithm is used to adjust the coordinates of each vertex. Since we restrict all vertexes
 * within a frame smaller than the input frame, we need to adjust the layout output to make the 
 * final layout center in the input frame.
 * @author xue
 *
 */
public class CoordinatesAdjust extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public CoordinatesAdjust(){
    super();
  }
  /**
   * Mapper.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Get displacements for x and y.
      int x_disp = Integer.parseInt(context.getParameter(ConstantLabels.X_DISP));
      int y_disp = Integer.parseInt(context.getParameter(ConstantLabels.Y_DISP));
      // Get the coordinates of this vertex.
      double x_coordinate = ((DoubleWritable)value.getLabel(ConstantLabels.X_COORDINATE)).get();
      double y_coordinate = ((DoubleWritable)value.getLabel(ConstantLabels.Y_COORDINATE)).get();
      x_coordinate += x_disp;
      y_coordinate += y_disp;
      
      // Update the coordinates.
      value.setLabel(ConstantLabels.X_COORDINATE, new DoubleWritable(x_coordinate));
      value.setLabel(ConstantLabels.Y_COORDINATE, new DoubleWritable(y_coordinate));
      output.collect(key, value);
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, CoordinatesAdjust.class);
    conf.setJobName("CoordinatesAdjust");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LabeledAdjSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner or reducer is needed..
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
    conf.setNumReduceTasks(0);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

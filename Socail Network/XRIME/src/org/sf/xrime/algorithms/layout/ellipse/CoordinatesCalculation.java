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
 * This algorithm is used to calculate the coordinates of each vertex according to its
 * sequential number.
 * @author liu chang yan
 */
public class CoordinatesCalculation extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public CoordinatesCalculation(){
    super();
  }

  /**
   * Calculate the coordinate for each vertex according to its sequential number.
   * The number assignment is sequential, but this calculation could be parallel.
   * @author liu chang yan
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Get the specified size of the display frame.
      int max_x = Integer.parseInt(context.getParameter(ConstantLabels.MAX_X_COORDINATE));
      int max_y = Integer.parseInt(context.getParameter(ConstantLabels.MAX_Y_COORDINATE));
      int num_of_vertexes = Integer.parseInt(context.getParameter(ConstantLabels.NUM_OF_VERTEXES));
      
      // We assume the coordinates are counted as the numbers of pixels from the upper-left corner
      // of the display frame.
      int origin_x = max_x/2;
      int origin_y = max_y/2;
      // 9/10 is used in order to keep distance from edges of the display frame.
      // radius1,radius are the axes of the ellipse
      int radius1 = max_y/2 -max_y/2/10;
      int radius2 = max_x/2 -max_x/2/10;
      // Calcualte coordinate according to vertex offset angle.
      int seq_num = ((IntWritable)value.getLabel(ConstantLabels.SEQUENTIAL_NUM)).get();
      int x_coordinate = (int)(origin_x + radius2 * Math.cos(seq_num * ((2*Math.PI)/num_of_vertexes)));
      int y_coordinate = (int)(origin_y - radius1 * Math.sin(seq_num * ((2*Math.PI)/num_of_vertexes)));
      LabeledAdjSetVertex result = new LabeledAdjSetVertex((AdjSetVertex)value);
      
      result.setLabel(ConstantLabels.X_COORDINATE, new IntWritable(x_coordinate));
      result.setLabel(ConstantLabels.Y_COORDINATE, new IntWritable(y_coordinate));
      output.collect(key, result);
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, CoordinatesCalculation.class);
    conf.setJobName("CoordinatesCalculation");
    
    conf.setOutputKeyClass(Text.class);
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
    
    // Only one mapper is permitted.
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(0);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

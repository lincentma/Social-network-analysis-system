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
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
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
import org.sf.xrime.algorithms.layout.ConstantLabels;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * This algorithm is used to summarize displacements caused by repulsive force (this part has
 * already been summarized in RepulsiveForceDisp) and attractive force (this part need to be
 * summarized and the added to the repulsive part).
 * @author xue
 */
public class DisplacementSummarize extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public DisplacementSummarize(){
    super();
  }
  /**
   * Mapper. Just re-emit it.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      output.collect(key, value);
    }
  }
  
  /**
   * Calculate new coordinates for each vertex.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Get the specified size of the display frame.
      int max_x = Integer.parseInt(context.getParameter(ConstantLabels.MAX_X_COORDINATE));
      int max_y = Integer.parseInt(context.getParameter(ConstantLabels.MAX_Y_COORDINATE));
      // Get the temperature.
      int temperature = Integer.parseInt(context.getParameter(ConstantLabels.TEMPERATURE));
      
      LabeledAdjSetVertex result = null;
      PlaneVector v_pos = null;
      PlaneVector v_disp = new PlaneVector(0,0);
      while(values.hasNext()){
        LabeledAdjSetVertex curr_vertex = values.next();
        if(curr_vertex.getLabel(ConstantLabels.X_COORDINATE)!=null){
          // This is the main one.
          // Create a new LabeledAdjVertex.
          result = new LabeledAdjSetVertex((AdjSetVertex)curr_vertex);
          // Determine the original coordinates.
          double x_coordinate = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.X_COORDINATE)).get();
          double y_coordinate = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.Y_COORDINATE)).get();
          v_pos = new PlaneVector(x_coordinate, y_coordinate);
          
          // Determine the displacement.
          double x_disp = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.X_DISP)).get();
          double y_disp = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.Y_DISP)).get();
          PlaneVector temp_vec = new PlaneVector(x_disp, y_disp);
          v_disp = v_disp.plus(temp_vec);
        }else{
          // Determine the displacement.
          double x_disp = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.X_DISP)).get();
          double y_disp = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.Y_DISP)).get();
          PlaneVector temp_vec = new PlaneVector(x_disp, y_disp);
          v_disp = v_disp.plus(temp_vec);
        }
      }
      
      if(v_pos == null || result == null){
        return; // Something wrong.
      }
      
      // Update the coordinates of this vertex.
      v_pos = v_pos.plus(v_disp.normalize().multiply_scalar(
          v_disp.magnitude()>temperature?temperature:v_disp.magnitude()));
      
      double new_x_coordinate = -1;
      double new_y_coordinate = -1;
      if(max_x>v_pos.getX()){
        if(v_pos.getX()>0){
          new_x_coordinate = v_pos.getX();
        }else{
          new_x_coordinate = 0;
        }
      }else{
        new_x_coordinate = max_x;
      }
      if(max_y>v_pos.getY()){
        if(v_pos.getY()>0){
          new_y_coordinate = v_pos.getY();
        }else{
          new_y_coordinate = 0;
        }
      }else{
        new_y_coordinate = max_y;
      }
      
      result.setLabel(ConstantLabels.X_COORDINATE, new DoubleWritable(new_x_coordinate));
      result.setLabel(ConstantLabels.Y_COORDINATE, new DoubleWritable(new_y_coordinate));
      
      // Collect it.
      output.collect(key, result);
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, DisplacementSummarize.class);
    conf.setJobName("DisplacementSummarize");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LabeledAdjSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is permitted, since the logic of reducer depends on the completeness
    // of information.
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
}

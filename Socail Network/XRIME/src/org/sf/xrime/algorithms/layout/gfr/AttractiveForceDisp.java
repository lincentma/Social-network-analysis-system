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
import java.util.LinkedList;

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
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * This algorithm is used to calculate vertex displacement caused by attractive force. The
 * parallelism is at the vertex level (we have assured that each edge is only counted once,
 * i.e., counted on the lexically smaller end).
 * @author xue
 *
 */
public class AttractiveForceDisp extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public AttractiveForceDisp(){
    super();
  }
  
  /**
   * Mapper. Accumulate coordinates of a vertex and its lexically larger neighbours at the vertex. 
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Re-emit myself first.
      output.collect(key, value);
      
      // Then notify some of my neighbours.
      for(AdjVertexEdge oppo : value.getOpposites()){
        if(key.toString().compareTo(oppo.getOpposite())>0){
          // The id of this vertex is lexically larger than the id of opposite vertex. In order to assure
          // that each edge is only count once, we use this trick.
          LabeledAdjSetVertex notifier = new LabeledAdjSetVertex();
          notifier.setId(oppo.getOpposite());
          // Tell the opposite vertex about my id, my coordinates.
          notifier.setStringLabel(ConstantLabels.OPPO_ID, key.toString());
          notifier.setLabel(ConstantLabels.X_COORDINATE, value.getLabel(ConstantLabels.X_COORDINATE));
          notifier.setLabel(ConstantLabels.Y_COORDINATE, value.getLabel(ConstantLabels.Y_COORDINATE));
          // Notify it.
          output.collect(new Text(oppo.getOpposite()), notifier);
        }
      }
    }
  }
  
  /**
   * Reducer. Calculate displacements of both ends of each edge. The displacement caused by attractive force
   * need to be accummulated in yet another round of map-reduce.
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
      // Get the number of vertexes.
      int num_of_vertexes = Integer.parseInt(context.getParameter(ConstantLabels.NUM_OF_VERTEXES));
      // Calculate the parameter k.
      double k = UtilityFunctions.k(max_x, max_y, num_of_vertexes);
      
      // We assume from end is the vertex with lexically smaller id.
      String from_id = null;
      PlaneVector from_end = null;
      LinkedList<String> to_ids = new LinkedList<String>();
      LinkedList<PlaneVector> to_ends = new LinkedList<PlaneVector>();
      
      while(values.hasNext()){
        LabeledAdjSetVertex curr_vertex = values.next();
        if(curr_vertex.getStringLabel(ConstantLabels.OPPO_ID)==null){
          // This is the from end.
          // Collect it first.
          output.collect(key, curr_vertex);
          // Record the id.
          from_id = key.toString();
          // Record the coordinates of the from end as a plane vector.
          double x_coordinate = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.X_COORDINATE)).get();
          double y_coordinate = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.Y_COORDINATE)).get();
          from_end = new PlaneVector(x_coordinate, y_coordinate);
        }else{
          // This is a to end.
          // Record the id.
          to_ids.add(curr_vertex.getStringLabel(ConstantLabels.OPPO_ID));
          // Record the coordinates.
          double x_coordinate = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.X_COORDINATE)).get();
          double y_coordinate = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.Y_COORDINATE)).get();
          to_ends.add(new PlaneVector(x_coordinate, y_coordinate));
        }
      }
      
      if(from_id == null || from_end == null) return; // Something wrong.
      
      // Calculate attractive force on each edge.
      for(int i=0; i<to_ends.size(); i++){
        String u_id = to_ids.get(i);
        PlaneVector u_pos = to_ends.get(i);
        PlaneVector delta = from_end.minus(u_pos);
        // We can only emit delta displacement here. The final summarization need another round of
        // map-reduce.
        PlaneVector v_delta_disp = delta.normalize().multiply_scalar((-1)*
                                   UtilityFunctions.f_a(k, delta.magnitude()));
        PlaneVector u_delta_disp = delta.normalize().multiply_scalar(
                                   UtilityFunctions.f_a(k, delta.magnitude()));
        // Emit these delta displacements.
        LabeledAdjSetVertex v_notifier = new LabeledAdjSetVertex();
        v_notifier.setId(from_id);
        v_notifier.setLabel(ConstantLabels.X_DISP, new DoubleWritable(v_delta_disp.getX()));
        v_notifier.setLabel(ConstantLabels.Y_DISP, new DoubleWritable(v_delta_disp.getY()));
        output.collect(new Text(from_id), v_notifier);
        
        LabeledAdjSetVertex u_notifier = new LabeledAdjSetVertex();
        u_notifier.setId(u_id);
        u_notifier.setLabel(ConstantLabels.X_DISP, new DoubleWritable(u_delta_disp.getX()));
        u_notifier.setLabel(ConstantLabels.Y_DISP, new DoubleWritable(u_delta_disp.getY()));
        output.collect(new Text(u_id), u_notifier);
      }
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, AttractiveForceDisp.class);
    conf.setJobName("AttractiveForceDisp");
    
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

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
import java.util.HashMap;
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
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * This algorithm is used to calculate the displacement caused by repulsive force
 * for each vertex. The parallelism is at the grid box level.
 * @author xue
 */
public class RepulsiveForceDisp extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public RepulsiveForceDisp(){
    super();
  }
  
  /**
   * Mapper. Aggregate vertexes according to grid boxes, so that all vertexes involved in
   * calculating repulsive force are collected. Then we can calculate repulsive forces in
   * reducer. 
   * @author xue
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
      // Get the number of vertexes.
      int num_of_vertexes = Integer.parseInt(context.getParameter(ConstantLabels.NUM_OF_VERTEXES));
      // Calculate the parameter k.
      double k = UtilityFunctions.k(max_x, max_y, num_of_vertexes);
      // Calculate the maximum grid x and grid y indexes.
      int max_grid_x_index = UtilityFunctions.grid_x_index(max_x-1, k);
      int max_grid_y_index = UtilityFunctions.grid_y_index(max_y-1, k);
      
      // Get the coordinates of this vertex.
      double x_coordinate = ((DoubleWritable)value.getLabel(ConstantLabels.X_COORDINATE)).get();
      double y_coordinate = ((DoubleWritable)value.getLabel(ConstantLabels.Y_COORDINATE)).get();
      // Get the x index of the grid surrounding this vertex.
      int grid_x_index = UtilityFunctions.grid_x_index(x_coordinate, k);
      int grid_y_index = UtilityFunctions.grid_y_index(y_coordinate, k);
      
      // Collect this vertex first.
      output.collect(new Text(UtilityFunctions.grid_index_as_string(grid_x_index, grid_y_index)), 
                     value);
      
      // Create a temp vertex as notifier.
      LabeledAdjSetVertex temp_vertex = new LabeledAdjSetVertex();
      temp_vertex.setId(key.toString());
      temp_vertex.setLabel(ConstantLabels.X_COORDINATE, new DoubleWritable(x_coordinate));
      temp_vertex.setLabel(ConstantLabels.Y_COORDINATE, new DoubleWritable(y_coordinate));
      temp_vertex.setStringLabel(ConstantLabels.SURROUNDING, "true");
      
      // Upper left.
      if(grid_x_index > 0 && grid_y_index > 0){
        String upper_left = UtilityFunctions.grid_index_as_string(grid_x_index - 1, grid_y_index - 1);
        output.collect(new Text(upper_left), temp_vertex);
      }
      // Upper.
      if(grid_y_index > 0){
        String upper = UtilityFunctions.grid_index_as_string(grid_x_index, grid_y_index - 1);
        output.collect(new Text(upper), temp_vertex);        
      }
      // Upper right.
      if(grid_x_index < max_grid_x_index && grid_y_index > 0){
        String upper_right = UtilityFunctions.grid_index_as_string(grid_x_index + 1, grid_y_index - 1);
        output.collect(new Text(upper_right), temp_vertex);
      }
      // Left.
      if(grid_x_index > 0){
        String left = UtilityFunctions.grid_index_as_string(grid_x_index - 1, grid_y_index);
        output.collect(new Text(left), temp_vertex);
      }
      // Right.
      if(grid_x_index < max_grid_x_index){
        String right = UtilityFunctions.grid_index_as_string(grid_x_index + 1, grid_y_index);
        output.collect(new Text(right), temp_vertex);
      }
      // Lower left.
      if(grid_x_index > 0 && grid_y_index < max_grid_y_index){
        String lower_left = UtilityFunctions.grid_index_as_string(grid_x_index - 1, grid_y_index + 1);
        output.collect(new Text(lower_left), temp_vertex);
      }
      // Lower.
      if(grid_y_index < max_grid_y_index){
        String lower = UtilityFunctions.grid_index_as_string(grid_x_index, grid_y_index + 1);
        output.collect(new Text(lower), temp_vertex);
      } 
      // Lower right.
      if(grid_x_index < max_grid_x_index && grid_y_index < max_grid_y_index){
        String lower_right = UtilityFunctions.grid_index_as_string(grid_x_index + 1, grid_y_index + 1);
        output.collect(new Text(lower_right), temp_vertex);
      }
    }
  }

  /**
   * Reducer. Calculate displacement of each vertex caused by repulsive force. Each time processes
   * vertexes within a grid box.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{
    /**
     * A private class used by this algorithm.
     * @author xue
     */
    private static class Triple {
      @SuppressWarnings("unused")
      public String id;
      public double x;
      public double y;
    }
    /**
     * Get vertex v and u ready.
     * @param values
     * @param vertexes_in_box
     * @param ids_involved
     * @param x_involved
     * @param y_involved
     */
    private void accummulate_data(Iterator<LabeledAdjSetVertex> values, 
        HashMap<String, LinkedList<LabeledAdjSetVertex>> vertexes_in_box,
        HashMap<String, LinkedList<Triple>> vertexes_involved){
      
      // Get v and u ready.
      while(values.hasNext()){
        LabeledAdjSetVertex curr_vertex = values.next();
        if(curr_vertex.getStringLabel(ConstantLabels.SURROUNDING)==null){
          // Here is the vertexes within this grid box.
          LabeledAdjSetVertex new_vertex = new LabeledAdjSetVertex((AdjSetVertex)curr_vertex);
          new_vertex.setLabel(ConstantLabels.X_COORDINATE, curr_vertex.getLabel(ConstantLabels.X_COORDINATE));
          new_vertex.setLabel(ConstantLabels.Y_COORDINATE, curr_vertex.getLabel(ConstantLabels.Y_COORDINATE));
          
          // Determine the coordinates.
          double x = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.X_COORDINATE)).get();
          double y = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.Y_COORDINATE)).get();
          
          // Create a string as key.
          String coordinate_str = "("+x+","+y+")";
          
          // Accummulate this vertex in the hash map.
          if(vertexes_in_box.get(coordinate_str)==null){
            // Create a new linked list.
            LinkedList<LabeledAdjSetVertex> new_list = new LinkedList<LabeledAdjSetVertex>();
            new_list.add(new_vertex);
            // Add it in the hash map.
            vertexes_in_box.put(coordinate_str, new_list);
          }else{
            vertexes_in_box.get(coordinate_str).add(new_vertex);
          }
 
          // Create a triple.
          Triple new_triple = new Triple();
          new_triple.id = curr_vertex.getId();
          new_triple.x = x;
          new_triple.y = y;
          // Record it.
          if(vertexes_involved.get(coordinate_str)==null){
            // Create a new linked list.
            LinkedList<Triple> new_list = new LinkedList<Triple>();
            new_list.add(new_triple);
            // Add the triple into the hash map.
            vertexes_involved.put(coordinate_str, new_list);
          }else{
            vertexes_involved.get(coordinate_str).add(new_triple);
          }
        }else{
          // Vertexes within surrounding grid boxes.
          double x = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.X_COORDINATE)).get();
          double y = ((DoubleWritable)curr_vertex.getLabel(ConstantLabels.Y_COORDINATE)).get();
          
          // Create a string as key.
          String coordinate_str = "("+x+","+y+")";
          
          // Create a triple.
          Triple new_triple = new Triple();
          new_triple.id = curr_vertex.getId();
          new_triple.x = x;
          new_triple.y = y;
          // Record it.
          if(vertexes_involved.get(coordinate_str)==null){
            // Create a new linked list.
            LinkedList<Triple> new_list = new LinkedList<Triple>();
            new_list.add(new_triple);
            // Add the triple into the hash map.
            vertexes_involved.put(coordinate_str, new_list);
          }else{
            vertexes_involved.get(coordinate_str).add(new_triple);
          }
        }
      }
    }

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
      
      // This hash map is used to accommodate vertexes in the box. We use a hash map since multiple
      // vertexes may have the same coordinates.
      HashMap<String, LinkedList<LabeledAdjSetVertex>> vertexes_in_box = 
        new HashMap<String, LinkedList<LabeledAdjSetVertex>>();
      // This hash map is used to accommodate vertexes involved in repulsive force calculation for
      // this box.
      HashMap<String, LinkedList<Triple>> vertexes_involved = new HashMap<String, LinkedList<Triple>>();
      
      // Get vertexes u and v ready.
      accummulate_data(values, vertexes_in_box, vertexes_involved);
      
      // Use the thread name as an indicator.
      Thread.currentThread().setName("XRIME-"+key.toString()+":"+vertexes_in_box.size()+":"+vertexes_involved.size());
      
      // Loop over all vertexes in this grid box.
      for(Iterator<String> iterator_in_box = vertexes_in_box.keySet().iterator(); iterator_in_box.hasNext();){
        // Get the list of vertexes in this box which have the same coordinates.
        LinkedList<LabeledAdjSetVertex> curr_in_box_list = vertexes_in_box.get(iterator_in_box.next());
        
        // Get the position of v, which represents potential a list of vertexes.
        LabeledAdjSetVertex v_vertex = curr_in_box_list.get(0);
        // String v_id = v_vertex.getId();
        double v_x = ((DoubleWritable)v_vertex.getLabel(ConstantLabels.X_COORDINATE)).get();
        double v_y = ((DoubleWritable)v_vertex.getLabel(ConstantLabels.Y_COORDINATE)).get();
        PlaneVector v_pos = new PlaneVector(v_x, v_y);
        
        // Newly create a displacement vector.
        PlaneVector v_disp = new PlaneVector(0,0);
        
        // Loop over all vertexes in this grid box and surronding grid boxes.
        for(Iterator<String> iterator_involved = vertexes_involved.keySet().iterator(); iterator_involved.hasNext();){
          // Get the list of vertexes involved which have the same coordinates.
          LinkedList<Triple> curr_involved_list = vertexes_involved.get(iterator_involved.next());
          
          // Get the position of u.
          Triple u_triple = curr_involved_list.get(0);
          // String u_id = curr_triple.id;
          double u_x = u_triple.x;
          double u_y = u_triple.y;
          PlaneVector u_pos = new PlaneVector(u_x, u_y);

          // In case when these two lists of vertexes overlap. Sometimes it just happens.
          if(v_x==u_x && v_y==u_y){
            // Skip, will be taken care of at the end of outer loop.
            continue;
          }
          
          // Since we have skipped all vertexes with same coordinates, we are sure that there is no
          // vertexes with same identifier now. Now we calculate displacement caused by repulsive 
          // force.
          
          // Calculate the delta.
          PlaneVector delta = v_pos.minus(u_pos);
          
          // If the distance larger than 2k, ignore this replusive force.
          if(delta.magnitude()>=(2*k)){
            // Ignore this repulsive force.
          }else{
            // Calculate new displacement of v. Note that the base displacement will be multiplied with
            // number of vertexes in curr_involved_list.
            try{
              v_disp = v_disp.plus(delta.normalize().
                  multiply_scalar(UtilityFunctions.f_r(k, delta.magnitude())).
                  multiply_scalar(curr_involved_list.size()));
            }catch(ArithmeticException e){
              // Possibly divide by zero. Do nothing for now.
            }
          }
        }
        
        if(curr_in_box_list.size()>1){
          // We have multiple overlapped vertexes in box, need to separate them. Try to arrange them
          // along a circle.
          
          // Determine the magnitude of the displacement of this vertex.
          double radius_base = v_disp.magnitude();
          // The radius used to calculate the delta for each overlapped vertex. This is a tricky point.
          double radius = (radius_base/10) > (k/4) ? radius_base/10 : k/4;
          
          for(int i=0; i<curr_in_box_list.size(); i++){
            // Deal with each vertex with the same coordinates.
            LabeledAdjSetVertex vert = curr_in_box_list.get(i);
            // Determine an adjustment vertex.
            PlaneVector adjust_vec = new PlaneVector(
                radius*Math.cos(i * ((2*Math.PI)/curr_in_box_list.size())),
                -radius*Math.sin(i * ((2*Math.PI)/curr_in_box_list.size())));
            // Coordinates after adjustment.
            PlaneVector vert_disp = v_disp.plus(adjust_vec);
            // Output the displacement of v along with its original information.
            vert.setLabel(ConstantLabels.X_DISP, new DoubleWritable(vert_disp.getX()));
            vert.setLabel(ConstantLabels.Y_DISP, new DoubleWritable(vert_disp.getY()));
            // Collect this.
            output.collect(new Text(vert.getId()), vert);
          }
        }else{
          // Get this only vertex.
          LabeledAdjSetVertex vert = curr_in_box_list.get(0);
          // Output the displacement of v along with its original information.
          vert.setLabel(ConstantLabels.X_DISP, new DoubleWritable(v_disp.getX()));
          vert.setLabel(ConstantLabels.Y_DISP, new DoubleWritable(v_disp.getY()));
          // Collect this.
          output.collect(new Text(vert.getId()), vert);
        }
      }
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, RepulsiveForceDisp.class);
    conf.setJobName("RepulsiveForceDisp");
    
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

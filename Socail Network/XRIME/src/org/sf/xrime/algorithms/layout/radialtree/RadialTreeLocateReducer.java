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
package org.sf.xrime.algorithms.layout.radialtree;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.sf.xrime.algorithms.layout.ConstantLabels;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * Reducer of RadialTree Layout Algorithm to caculate the coordinates of vertex. Using
 * vertex angle scale caculated by weight, get the coordinates and its successive vertices
 * angle sclae. Emit the vertex to its successive vertices.
 * @author liu chang yan
 */
public class RadialTreeLocateReducer extends GraphAlgorithmMapReduceBase 
  implements Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  
  JobConf job=null;
  Text outputKey=new Text();
  boolean changeFlag=false;  
  
  public void reduce(Text key, Iterator<LabeledAdjVertex> values,
    OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
    throws IOException {
      
    long now_distance = Long.parseLong((context.getParameter(ConstantLabels.NUM_OF_VERTEXES)));     
    long max_distance = Long.parseLong((context.getParameter(ConstantLabels.START_SEQ_NUM)));     
    int max_x = Integer.parseInt(context.getParameter(ConstantLabels.MAX_X_COORDINATE));
    int max_y = Integer.parseInt(context.getParameter(ConstantLabels.MAX_Y_COORDINATE));
    int radius = Integer.parseInt(context.getParameter(ConstantLabels.TEMPERATURE));
       
    LabeledAdjVertex initRadialTreeVertex=null;
    LabeledAdjVertex visitRadialTreeVertex=null;
    RadialTreeLabel initLabel = new RadialTreeLabel();
    RadialTreeLabel visitLabel = new RadialTreeLabel();
    
    double angle = 0;
    // Choose the vertex itself and its previous vertices
    while(values.hasNext()){
      LabeledAdjVertex vertex=new LabeledAdjVertex(values.next());
      RadialTreeLabel label=(RadialTreeLabel) vertex.getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);
        
      if(vertex.getId().compareTo(key.toString()) != 0){          
        visitLabel = label;
        visitRadialTreeVertex = vertex;
      }
      if(vertex.getId().compareTo(key.toString()) == 0){
        initRadialTreeVertex = vertex;
        initLabel = label;
      }                
    }
    // Deal with label
    if(visitRadialTreeVertex != null){        
      initLabel.setAngle_from(visitLabel.getSucc_angle_from());
      initLabel.setAngle_begin(visitLabel.getSucc_angle_from());
      initLabel.setAngle_to(visitLabel.getSucc_angle_to());
      initLabel.setSucc_angle_from(visitLabel.getSucc_angle_from());        
      initLabel.setSucc_angle_to(visitLabel.getSucc_angle_to());          
    }  
    // Choose the middle of the angle scale
    angle = ((initLabel.getAngle_to() - initLabel.getAngle_from()) / 2) + initLabel.getAngle_from();
    // Caculate the coordinates of vertex
    if(initLabel.getDistance() == now_distance) {
      initLabel.setCordinate_x((int) (max_x / 2 + (0.5*radius*now_distance*Math.cos(angle))));
      initLabel.setCordinate_y((int) (max_y / 2 + (0.5*radius*now_distance*Math.sin(angle))));
    }
    
    // Judge whether remove the old label and add a new label
    if(now_distance <= max_distance){
      output.collect(key, initRadialTreeVertex);
    }else{
      int x_coordinate = initLabel.getCoordinate_x();
      int y_coordinate = initLabel.getCoordinate_y();
      initRadialTreeVertex.removeLabel(RadialTreeLabel.RadialTreeLabelPathsKey);        
      initRadialTreeVertex.setLabel(ConstantLabels.X_COORDINATE, new IntWritable(x_coordinate));
      initRadialTreeVertex.setLabel(ConstantLabels.Y_COORDINATE, new IntWritable(y_coordinate));      
      output.collect(key, initRadialTreeVertex);        
    }            
  }
  
  /** 
   * Keep JobConf to create FileSystem.
   * @see org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf job) {
    super.configure(job);
    this.job=job;
  }
}




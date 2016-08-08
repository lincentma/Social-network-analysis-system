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
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.sf.xrime.algorithms.layout.ConstantLabels;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * Mapper of RadialTree Layout Algorithm to caculate the coordinates of vertex and emit 
 * the vertices according to their distance.  
 * @author liu chang yan
 */
public class RadialTreeLocateMapper extends GraphAlgorithmMapReduceBase implements Mapper<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  
  JobConf job=null;
  Text outputKey=new Text();

  @Override
  public void map(Text key, LabeledAdjVertex value,
      OutputCollector<Text, LabeledAdjVertex> collector, Reporter reporter)
      throws IOException {                   
    // Get the size of the display frame.
    int max_x = Integer.parseInt(context
        .getParameter(ConstantLabels.MAX_X_COORDINATE));
    int max_y = Integer.parseInt(context
        .getParameter(ConstantLabels.MAX_Y_COORDINATE));
    long now_distance = Long.parseLong((context
        .getParameter(ConstantLabels.NUM_OF_VERTEXES)));
    // Caculate vertex itself weight
    double scale = (max_x < max_y) ? max_x : max_y;
    double weight = (now_distance == 0) ? 0 : Math.sqrt(scale * scale)
        / now_distance;
    RadialTreeLabel label = (RadialTreeLabel) value
        .getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);
    List<String> iter = label.getSucc();

    // Initialize the root vertex
    if (label.getDistance() == 0) {
      label.setCordinate_x(max_x / 2);
      label.setCordinate_y(max_y / 2);
      label.setAngle_from(0);
      label.setAngle_begin(0);
      label.setAngle_to(Math.PI * 2.0);
      label.setSucc_angle_from(0);
      label.setSucc_angle_to(Math.PI * 2.0);
    }
    // Caculate successive vetices angle scale when this time is its turn
    if (label.getDistance() == now_distance) {
      for (int i = 0; i < iter.size(); i++) {
        outputKey.set(iter.get(i));
        i++;
        double angle_scale = Double.parseDouble(iter.get(i))
            / (label.getWeight() - weight)
            * (label.getAngle_to() - label.getAngle_from());
        label.setSucc_angle_from(label.getAngle_begin());
        label.setSucc_angle_to(label.getAngle_begin() + angle_scale);
        label.setAngle_begin(label.getSucc_angle_to());
        collector.collect(outputKey, value);
      }
    }
    collector.collect(key, value);         
  }
  
  public void configure(JobConf job) {
    super.configure(job);
    this.job=job;
  }  
}
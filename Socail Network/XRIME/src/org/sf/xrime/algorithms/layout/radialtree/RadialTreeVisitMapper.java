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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * Mapper of RadialTree Layout Algorithm to vist all the vertices. Do nothing than 
 * emit all input.
 * @author liu chang yan
 */
public class RadialTreeVisitMapper extends GraphAlgorithmMapReduceBase implements Mapper<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {

  @Override
  public void map(Text key, LabeledAdjVertex value,
      OutputCollector<Text, LabeledAdjVertex> collector, Reporter reporter)
      throws IOException {     
      collector.collect(key, value);    
  }
}

/*
 * Copyright (C) yangyin@BUPT. 2009.
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
package org.sf.xrime.algorithms.MST;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * This mapper class is used after the termination of the computing step of MST algorithm
 * to trim the unprocessed messages off but only keep information of the vertexes
 * @author YangYin
 * @see org.apache.hadoop.mapred.Mapper
 */
public class MSTFinalMapper extends GraphAlgorithmMapReduceBase implements Mapper<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  /**
   *  @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void map(Text key, LabeledAdjVertex value,
      OutputCollector<Text, LabeledAdjVertex> collector, Reporter reporter)
      throws IOException {
    /**
     * Equality means that this is a vertex but not a message
     * Keep the vertexes only
     */
    if((key.toString()).equals(value.getId()))
      collector.collect(key, value);
  }
}
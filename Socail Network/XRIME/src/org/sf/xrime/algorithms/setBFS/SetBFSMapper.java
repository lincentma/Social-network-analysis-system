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
package org.sf.xrime.algorithms.setBFS;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * Mapper of Set BFS. Do nothing than emit all input.
 * @author Cai Bin
 */
public class SetBFSMapper extends GraphAlgorithmMapReduceBase implements Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex> {
  /* (non-Javadoc)
   * map method of BFSMapper. Only need to emit the input.
   * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void map(Text key, LabeledAdjSetVertex value,
      OutputCollector<Text, LabeledAdjSetVertex> collector, Reporter reporter)
      throws IOException {
    collector.collect(key, value);
  }
}

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
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;


/**
 * This reduce class does nothing but only emit the input 
 * @author YangYin
 * @see org.apache.hadoop.mapred.Reducer
 */
public class MSTFinalReducer extends GraphAlgorithmMapReduceBase 
    implements Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
  
  JobConf job=null;
  
  boolean changeFlag=false;
  
  Text outputKey=new Text();  

  
  public void configure(JobConf job) {
    super.configure(job);
    this.job=job;
  }
  
  /**
   * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public void reduce(Text key, Iterator<LabeledAdjVertex> values,
      OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
      throws IOException {
    while (values.hasNext()) {
      LabeledAdjVertex vertex=new LabeledAdjVertex(values.next());
      output.collect(key, vertex);
    }
  }
}


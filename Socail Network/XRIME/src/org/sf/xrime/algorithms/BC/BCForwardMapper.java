/*
 * Copyright (C) yangcheng@BUPT. 2009.
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
package org.sf.xrime.algorithms.BC;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;;
/*
 * @author Yang Cheng
 * no need to do anything just to emit the vertexes
 */
public class BCForwardMapper extends GraphAlgorithmMapReduceBase implements Mapper<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex> {
  @Override
  public void map(Text key, LabeledAdjBiSetVertex value,
      OutputCollector<Text, LabeledAdjBiSetVertex> collector, Reporter reporter)
      throws IOException {
//    System.out.println("Here forward Mapper -------");
//    System.out.println("here key     "+ key.toString() +"          here value"+ value +"---------===========");
      collector.collect(key, value);
    
    
    
  }
}

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
package org.sf.xrime.postprocessing;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * A mapper used to transform incoming writable key-value pairs to text key-value pairs.
 */
public class SequenceFileToTextFileMapper extends MapReduceBase
  implements Mapper<Object, Object, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();
	
    @Override
    public void map(Object key, Object value, OutputCollector<Text, Text> collector,
        Reporter reporter) throws IOException {
      try {
        outputKey.set(key.toString());
        outputValue.set(value.toString());
        collector.collect(outputKey, outputValue);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
}

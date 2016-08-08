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
package org.sf.xrime.algorithms.partitions.connected.strongly;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
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
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
 * This class is used to extract partitions corresponding to SCCs.
 * @author xue
 */
public class ExtractPartitions extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public ExtractPartitions(){
    super();
  }
  /**
   * Mapper.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjBiSetVertex, Text, Text>{

    @Override
    public void map(Text key, LabeledAdjBiSetVertex value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      if(value.getStringLabel(ConstantLabels.FINAL_LABEL)!=null){
        // The normal case.
        output.collect(key, new Text(value.getStringLabel(ConstantLabels.FINAL_LABEL)));
      }else{
        // A special case, where the scc is a single vertex.
        output.collect(key, key);
      }
    }
  }
  
  /**
   * Reducer. Used to merge the map output into less files.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, Text, Text, Text>{

    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      while(values.hasNext()){
        output.collect(key, values.next());
      }
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, ExtractPartitions.class);
    conf.setJobName("ExtractPartitions");
 
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertexes (Writable)
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(MapClass.class);        
    // No combiners.
    conf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    try {
      Path[] paths = new Path[1];
      FileInputFormat.setInputPaths(conf, getSource().getPaths().toArray(paths));
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(getReducerNum());
    conf.setMapOutputCompressorClass(GzipCodec.class);
    conf.setCompressMapOutput(true);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

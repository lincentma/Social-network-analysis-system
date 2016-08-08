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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
 * This class is used to extract SCC from a label propagation converged graph. We may need
 * to extend Map-Reduce framework to support multiple output collector, so as to extract 
 * SCC, predecessors, descendants, and reminders in one iteration.
 * @author xue
 *
 */
public class ExtractSCC extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public ExtractSCC(){
    super();
  }
  /**
   * Mapper. Find those vertexes with both FORWARD_LABEL and BACKWARD_LABEL, which are
   * vertexes inside the SCC. Please recall that, we only propagate label from a single
   * source vertex in one iteration.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{

    @Override
    public void map(Text key, LabeledAdjBiSetVertex value,
        OutputCollector<Text, LabeledAdjBiSetVertex> output, Reporter reporter)
        throws IOException {
      // the forward label and backward label should be the same.
      if(value.getStringLabel(ConstantLabels.BACKWARD_LABEL)!=null &&
         value.getStringLabel(ConstantLabels.FORWARD_LABEL)!=null){
        // Create a new LabeledAdjBiSetVertex.
        LabeledAdjBiSetVertex result = new LabeledAdjBiSetVertex();
        // Set id.
        result.setId(key.toString());
        // Only set this labels.
        result.setStringLabel(ConstantLabels.FINAL_LABEL, value.getStringLabel(ConstantLabels.FORWARD_LABEL));
        output.collect(key, result);
      }
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, ExtractSCC.class);
    conf.setJobName("ExtractSCC");
 
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertexes (Writable)
    conf.setOutputValueClass(LabeledAdjBiSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiners, no reducers.
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(0);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

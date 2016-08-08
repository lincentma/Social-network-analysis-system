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
package org.sf.xrime.algorithms.BFS.alg_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.model.vertex.LabeledAdjVertex;


/**
 * Iterate, until we can not find new frontier.
 * @author weixue@cn.ibm.com
 */
public class BFSStep extends GraphAlgorithm {  

  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      JobConf jobConf = new JobConf(context, BFSStep.class);
      jobConf.setJobName("BFS");
  
      FileInputFormat.setInputPaths(jobConf, context.getSource().getPath());
        
      jobConf.setInputFormat(SequenceFileInputFormat.class);  
      jobConf.setMapperClass(BFSMapper.class);
      jobConf.setNumMapTasks(getMapperNum());        
      jobConf.setMapOutputValueClass(LabeledAdjVertex.class);    
      
      // jobConf.setCombinerClass(BFSCombineClass.class);
        
      jobConf.setReducerClass(BFSReducer.class);
      jobConf.setNumReduceTasks(getReducerNum());
        
      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(LabeledAdjVertex.class);
          
      FileOutputFormat.setOutputPath(jobConf, context.getDestination().getPath());
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);    
  
      this.runningJob = JobClient.runJob(jobConf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

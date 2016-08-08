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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * This class is the step class in the computing part of MST algorithm
 * The execution method of step class in MST algorithm is called in each
 * round of the algorithm  
 * @author YangYin
 *
 */
public class MSTStep extends GraphAlgorithm {

  private String terminateFileName="Terminate";
  static final public String terminateFileKey="xrime.algorithm.MST.terminate.flag";
  static final public String autoWakeUpListKey = "xrime.algorithm.MST.autoWakeUpList.flag";
  private boolean end = false;

  private JobConf jobConf;  
  private FileSystem client = null;
  
  private SequenceTempDirMgr tempDirs = null;
  private String autoWakeUpList = "";
  
  public String getTerminateFileFlag() {
    return terminateFileName;
  }

  public void setTerminateFileFlag(String terminateFileName) {
    this.terminateFileName = terminateFileName;
  }
  
  private String terminateFlagFile() throws IllegalAccessException {
    Path filePath=new Path(context.getDestination().getPath().toString());
    return filePath.getParent().toString() + "/" + terminateFileName;
  }
  
  public boolean isEnd() {
    return end;
  }  
  
  public FileSystem getClient() {
    return client;
  }

  public void setClient(FileSystem client) {
    this.client = client;
  }

  public String getAutoWakeUpList() {
    return autoWakeUpList;
  }

  public void setAutoWakeUpList(String autoWakeUpList) {
    this.autoWakeUpList = autoWakeUpList;
  }

  public SequenceTempDirMgr getTempDirs() {
    return tempDirs;
  }

  public void setTempDirs(SequenceTempDirMgr tempDirs) {
    this.tempDirs = tempDirs;
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      /**
       * Set related properties for MST job
       */
      context.setParameter(terminateFileKey, terminateFlagFile());
      context.setParameter(MSTStep.autoWakeUpListKey, autoWakeUpList);
      
      jobConf = new JobConf(context, MSTStep.class);
      jobConf.setJobName("MST");

      FileInputFormat.setInputPaths(jobConf, context.getSource().getPath());

      jobConf.setInputFormat(SequenceFileInputFormat.class);
      if (!isEnd())
        jobConf.setMapperClass(MSTMapper.class);
      else
        jobConf.setMapperClass(MSTFinalMapper.class);

      jobConf.setNumMapTasks(getMapperNum());
      jobConf.setMapOutputValueClass(LabeledAdjVertex.class);

      if (!isEnd())
        jobConf.setReducerClass(MSTReducer.class);
      else
        jobConf.setReducerClass(MSTFinalReducer.class);

      jobConf.setNumReduceTasks(getReducerNum());

      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(LabeledAdjVertex.class);

      FileOutputFormat.setOutputPath(jobConf, context.getDestination()
          .getPath());
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);


      this.runningJob = JobClient.runJob(jobConf);

      if (client == null) {
        client = FileSystem.get(jobConf);
      }
      if (client.exists(new Path(terminateFlagFile()))) {
        end = true;
      } else {
        end = false;
      }
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}

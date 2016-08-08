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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;
import org.sf.xrime.utils.SequenceTempDirMgr;

public class BCBackwardStep extends GraphAlgorithm {  
  private boolean end=false;
  private JobConf jobConf;  
  private FileSystem client=null;
  private SequenceTempDirMgr tempDirs=null;
  private int dist;
  /*
  public String getContinueFlag() {
    return continueFileName;
  }

  public void setContinueFlag(String continueFlag) {
    this.continueFileName = continueFlag;
  }
  
  private String continueFlagFile() throws IllegalAccessException {
    Path filePath=new Path(context.getDestination().getPath().toString()+"/"+continueFileName);
      return filePath.toString();
  }
  */
  public void setDistance(int distance)
  {
    dist=distance;
  }
  public int getDistance()
  {
    return dist;
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

  public SequenceTempDirMgr getTempDirs() {
    return tempDirs;
  }

  public void setTempDirs(SequenceTempDirMgr tempDirs) {
    this.tempDirs = tempDirs;
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      context.setParameter("distance", Integer.toString(dist));
      
      jobConf = new JobConf(context, BCBackwardStep.class);
      jobConf.setJobName("BC");

      jobConf.setMapperClass(BCBackwardMapper.class);
      jobConf.setReducerClass(BCBackwardReducer.class);
      
      jobConf.setMapOutputValueClass(LabeledAdjBiSetVertex.class);    
      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(LabeledAdjBiSetVertex.class);
      
      //jobConf.setNumMapTasks(getMapperNum());       
      jobConf.setNumMapTasks(1);
      //jobConf.setNumReduceTasks(getReducerNum());
      jobConf.setNumReduceTasks(1);
      
      jobConf.setInputFormat(SequenceFileInputFormat.class);  
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);    
      
      FileInputFormat.setInputPaths(jobConf, context.getSource().getPath());
      FileOutputFormat.setOutputPath(jobConf, context.getDestination().getPath());
      
      this.runningJob = JobClient.runJob(jobConf);
    
      if(dist>0)
      {
        end=false;
      }
      else
        end=true;
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}

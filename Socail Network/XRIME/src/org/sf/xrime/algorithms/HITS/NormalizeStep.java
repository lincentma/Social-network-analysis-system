/*
 * Copyright (C) quna@BUPT. 2009.
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
package org.sf.xrime.algorithms.HITS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;
import org.sf.xrime.utils.SequenceTempDirMgr;
import org.sf.xrime.algorithms.HITS.HITSSummer;

//import org.sf.xrime.algorithms.statistics.VertexEdgeDoubleCounter;

/**
* 
* @author Qu Na
* 
*/

public class NormalizeStep extends GraphAlgorithm {
  private String continueFileName = "continue";
  static final public String continueFileKey = "xrime.algorithm.HITS.continue.flag";
  private boolean end = false;
  private HITSSummer summer;
  private Configuration conf;
  private JobConf jobConf;
  private FileSystem client = null;
  
  @SuppressWarnings("unused")
  private double stopThreshold = 1/202024;
  
  private SequenceTempDirMgr tempDirs = null;
  
  public NormalizeStep(HITSSummer summer){
    super();
    this.summer= summer;
  }
  public String getContinueFlag(){
    return continueFileName;
  }
  
  public void setContinueFlag(String continueFlag){
    this.continueFileName = continueFlag;
  }
  
  private String continueFlagFile() throws IllegalAccessException{
    Path filePath = new Path(context.getDestination().getPath().toString() + "/" + continueFileName);
    return filePath.toString();
  }
  
  public boolean isEnd(){
    return end;
  }
  
  public FileSystem getClient(){
    return client;
  }
  
  public void setClient(FileSystem client){
    this.client = client;
  }
  
  public SequenceTempDirMgr getTempDirs(){
    return tempDirs;
  }
  
  public void setTempDirs(SequenceTempDirMgr temDirs){
    this.tempDirs = temDirs;
  }
  
  public void setStopThreshold(double stopThreshold) {
    this.stopThreshold = stopThreshold;
  }
  
  /*
   * public double getStopThreshold(){ return stopThreshold; }
   */

  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      context.setParameter(continueFileKey, continueFlagFile());

      // HITSSummer summer
      // summer.getHubSum();
      context.setParameter(HITSSummer.hubCounterKey, Double.toString(summer.getHubSum()));
      context.setParameter(HITSSummer.authorityCounterKey, Double.toString(summer.getAuthoritySum()));
      // prop.setProperty(HITSSummer.hubCounterKey,
      // Double.toString(VertexEdgeDoubleCounter.hubsummer));
      // prop.setProperty(HITSSummer.authorityCounterKey,
      // Double.toString(VertexEdgeDoubleCounter.authoritysummer));
      jobConf = new JobConf(conf, NormalizeStep.class);
      jobConf.setJobName("Normalize");

      FileInputFormat.setInputPaths(jobConf, context.getSource().getPath());

      jobConf.setInputFormat(SequenceFileInputFormat.class);
      jobConf.setMapperClass(NormalizeMapper.class);
      jobConf.setNumMapTasks(getMapperNum());
      jobConf.setMapOutputValueClass(LabeledAdjBiSetVertex.class);

      // jobConf.setReducerClass(NormalizeReducer.class);
      // jobConf.setNumReduceTasks(getReducerNum());

      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(LabeledAdjBiSetVertex.class);

      FileOutputFormat.setOutputPath(jobConf, context.getDestination().getPath());
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);

      this.runningJob = JobClient.runJob(jobConf);

      if (client == null) {
        client = FileSystem.get(jobConf);
      }

      if (client.exists(new Path(continueFlagFile()))) {
        end = false;
        client.delete(new Path(continueFlagFile()), true);
      } else {
        end = true;
      }
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}

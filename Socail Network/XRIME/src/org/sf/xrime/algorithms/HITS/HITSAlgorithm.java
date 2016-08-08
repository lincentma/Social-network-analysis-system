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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.model.Graph;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
* 
* @author Qu Na
* 
*/

public class HITSAlgorithm extends GraphAlgorithm{
  static final public String HITSHubKey = "xrime.algorithm.HITS.Hub";
  static final public String HITSAuthorityKey = "xrime.algorithm.HITS.Authority";
  //static final public String HITSHubSumKey = "xrime.algorithm.HITS.HubSum";
  //static final public String HITSAuthoritySumKey = "xrime.algorithm.HITS.AuthoritySum"; 
  static final public String HITSStopThresholdKey = "xrime.algorithm.HITS.stopThreshold";  
  
  private JobConf jobConf;
  private FileSystem client = null;
  private SequenceTempDirMgr tempDirs = null;
  
  private int maxStep = 10;
  private double stopThreshold = 1/202024;
  
  public FileSystem getClient(){
    return client;
  }
  
  public void setClient(FileSystem client){
    this.client = client;
  }
  
  public SequenceTempDirMgr getTempDirs(){
    return tempDirs;
  }
  
  public void setTempDirs(SequenceTempDirMgr tempDirs){
    this.tempDirs = tempDirs;
  }
  
  public int getMaxStep(){
    return maxStep;
  }
  
  public void setMaxStep(int maxStep){
    this.maxStep = maxStep; 
  }
  
  public double getStopThreshold(){
    return stopThreshold;
  }
  
  public void setStopthreshold(double stopThreshold){
    this.stopThreshold = stopThreshold;
  }
  
  public Path getSrcPath(){
    Graph src = context.getSource();
    if(src == null){
      return null;
    }
    
    try{
      return src.getPath();
    }catch(IllegalAccessException e){
      return null;
    }
  }
  
  public void setSrcPath(Path srcPath){
    Graph src = context.getSource();
    if(src == null){
      src = new Graph(Graph.defaultGraph());
      context.setSource(src);
    }
    src.setPath(srcPath);
  }
  
  public Path getDestPath(){
    Graph dest = context.getDestination();
    if(dest == null){
      return null;
    }
    
    try{
      return dest.getPath();
    }catch(IllegalAccessException e){
      return null;
    }
  }
  
  public void setDestPath(Path destPath){
    Graph dest = context.getDestination();
    if(dest == null){
      dest = new Graph(Graph.defaultGraph());
      context.setDestination(dest);
    }
    dest.setPath(destPath);
  }
  
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    if (params.length != 1) {
      throw new ProcessorExecutionException("Usage: xrime <in>");
    }

    setSrcPath(new Path(params[0]));
    setDestPath(new Path(params[0] + "Out"));
    setMaxStep(50);
    setStopthreshold(0.01);
  }

  @Override
  public void execute() throws ProcessorExecutionException{
    try{
      jobConf = new JobConf(context,HITSAlgorithm.class);
      
      client = FileSystem.get(jobConf);
      if(tempDirs == null){
        tempDirs = new SequenceTempDirMgr(context.getDestination().getPath());
      }
      tempDirs.setFileSystem(client);
      
      DeliveryStep step1 = new DeliveryStep();
      step1.setConf(context);
      step1.getContext().setSource(getSource());
      Graph dest1 = new Graph(getSource());
      dest1.setPath(tempDirs.getTempDir());
      step1.getContext().setDestination(dest1);
      
      HITSSummer summer = new HITSSummer();
      summer.setConf(context);
      NormalizeStep step2 = new NormalizeStep(summer);      
      step2.setConf(context);
      step2.setStopThreshold(stopThreshold);
      
      int step = maxStep;
      while(step>0){
        step1.execute();
        
        summer.setSource(step1.getContext().getDestination());
        Graph sumdest = new Graph(getSource());
        sumdest.setPath(tempDirs.getTempDir());
        summer.setDestination(sumdest);
        summer.execute();
        
        step2.setSource(step1.getContext().getDestination());
        Graph dest2 = new Graph(getSource());
        dest2.setPath(tempDirs.getTempDir());
        step2.getContext().setDestination(dest2);
        step2.execute();
        if(step2.isEnd()){
          break;
        }
        step1.setSource(step2.getContext().getDestination());
        step--;
        
        dest1.setPath(tempDirs.getTempDir());
      }
    }catch(IOException e){
      e.printStackTrace();
      throw new ProcessorExecutionException(e);
    }catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

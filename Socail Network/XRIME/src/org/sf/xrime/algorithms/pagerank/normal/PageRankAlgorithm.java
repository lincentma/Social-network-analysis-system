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
package org.sf.xrime.algorithms.pagerank.normal;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.pagerank.PageRankStep;
import org.sf.xrime.model.Graph;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * PageRank algorithm implementation. 
 * Please refer to http://en.wikipedia.org/wiki/PageRank. 
 * @see "The Anatomy of a Large-Scale Hypertextual Web Search Engine by L. Page and S. Brin, 1999"
 */
public class PageRankAlgorithm extends GraphAlgorithm {
  static final public String pageRankDampingFactorKey = "xrime.algorithm.pageRank.dampingFactor";
  static final public String pageRankStopThresholdKey = "xrime.algorithm.pageRank.stopThreshold";
  static final public String pageRankVertexAdjKey     = "xrime.algorithm.pageRank.adjust";
      
  protected JobConf jobConf;
  protected FileSystem client=null;  
  protected SequenceTempDirMgr tempDirs=null;
  
  protected int maxStep=10;
  protected double stopThreshold=0.01;
  protected double dampingFactor=1;

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
  
  public int getMaxStep() {
    return maxStep;
  }

  public void setMaxStep(int maxStep) {
    this.maxStep = maxStep;
  }

  public double getDampingFactor() {
    return dampingFactor;
  }

  public void setDampingFactor(double dampingFactor) {
    if(dampingFactor>=0 && dampingFactor<=1) {
      this.dampingFactor = dampingFactor;
      return;
    }
    
    throw new IllegalArgumentException("Damping Factor d should be 0<=d<1");
  }
  
  public double getStopThreshold() {
    return stopThreshold;
  }

  public void setStopThreshold(double stopThreshold) {
    this.stopThreshold = stopThreshold;
  }

  public Path getSrcPath() {
    Graph src=context.getSource();
    if(src==null) {
      return null;
    }
    
    try {
      return src.getPath();
    } catch (IllegalAccessException e) {
      return null;
    }
  }
  
  public void setSrcPath(Path srcPath) {
    Graph src=context.getSource();
    if(src==null) {
      src=new Graph(Graph.defaultGraph());
      context.setSource(src);
    }
    src.setPath(srcPath);
  }
  
  public Path getDestPath() {
    Graph dest=context.getDestination();
    if(dest==null) {
      return null;
    }
    
    try {
      return dest.getPath();
    } catch (IllegalAccessException e) {
      return null;
    }
  }
  
  public void setDestPath(Path destPath) {
    Graph dest=context.getDestination();
    if(dest==null) {
      dest=new Graph(Graph.defaultGraph());
      context.setDestination(dest);
    }
    dest.setPath(destPath);
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      jobConf = new JobConf(context, PageRankAlgorithm.class);
      
      client=FileSystem.get(jobConf);
      if(tempDirs==null) {
          tempDirs=new SequenceTempDirMgr(context.getDestination().getPath());
      }
      tempDirs.setFileSystem(client);
      tempDirs.setSeqNum(0);
      
      PageRankStep bfsStep=new PageRankStep();
      bfsStep.setConf(context);
      bfsStep.setSource(getSource());
      Graph dest=new Graph(getSource());
      dest.setPath(tempDirs.getTempDir());
      bfsStep.setDestination(dest);
      bfsStep.setStopThreshold(stopThreshold);
      bfsStep.setDampingFactor(dampingFactor);
      //bfsStep.setVertexNumber(counter.getCounter(GeneralCounterFilter.vertexCounterKey));

      int step=maxStep;
      while(step>0) {
        bfsStep.execute();
        if( bfsStep.isEnd() ) {
          break;
        }            
        
        bfsStep.setSource(bfsStep.getDestination());
        dest=new Graph(getSource());
        dest.setPath(tempDirs.getTempDir());
        bfsStep.setDestination(dest);
        step--;
      }
      bfsStep.stopServer();
      client.close();      
    } catch (IOException e) {
      e.printStackTrace();
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

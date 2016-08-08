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
import org.apache.hadoop.mapred.JobConf;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.model.Graph;
import org.sf.xrime.utils.SequenceTempDirMgr;

/**
 * MST Algorithm finds the minimum spanning tree of a connected 
 * large scale weighted undirected graph
 * @see org.sf.xrime.algorithms.GraphAlgorithm
 */
public class MSTAlgorithm extends GraphAlgorithm {

  private JobConf jobConf;
  
  private FileSystem client=null;  
  
  private SequenceTempDirMgr tempDirs=null;
  
  /**
   * autoWakeUpList indicates a list of vertexes that should wake up automatically
   */
  private String autoWakeUpList = "";
  
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
  
  /**
   * Final output file stores the final result of the MST algorithm
   * This function gives the path of the final result file
   * @return the path used to store the final result file
   * @throws IllegalAccessException
   */
  private String finalOutputFile() throws IllegalAccessException {
    Path filePath=new Path(context.getDestination().getPath().toString());
    return filePath.getParent().toString() + "/FinalOutput";
  }
  
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    String autoWakeUpList = "";
    if (params.length < 1) {
      throw new ProcessorExecutionException(
          "Usage: smthsocial <in>, use -i to specify the auto wakeup vertexes");
    }

    setSrcPath(new Path(params[0]));
    setDestPath(new Path(params[0] + "Output"));

    if (params.length >= 2) {
      if (params[1].equals("-i")) {
        for (int i = 2; i < params.length; i++) {
          autoWakeUpList = autoWakeUpList + params[i] + " ";
        }
        autoWakeUpList = autoWakeUpList.substring(0,
            autoWakeUpList.length() - 1);
      }
    }
    setAutoWakeUpList(autoWakeUpList);
  }

  /**
   * Run the MST algorithm, functionates controlling the process, 
   * detecting algorithm termination and so on
   */
  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      jobConf = new JobConf(context, MSTAlgorithm.class);
      client = FileSystem.get(jobConf);
      if(tempDirs == null) {
          tempDirs = new SequenceTempDirMgr(context.getDestination().getPath());
      }
      tempDirs.setFileSystem(client);
      
      /**
       * Initiate the step of MST algorithm, 
       * set source, set destionation and so on
       */
      MSTStep mstStep=new MSTStep();
      mstStep.setConf(context);
      mstStep.setSource(getSource());
      Graph dest=new Graph(getSource());
      dest.setPath(tempDirs.getTempDir());
      mstStep.setDestination(dest);
      mstStep.setAutoWakeUpList(autoWakeUpList);
      /**
       * Algorithm continues until termination
       */
      while(!mstStep.isEnd()) {
        /**
         * one round execution of MST algorithm
         */
        mstStep.execute();
        /**
         * set the new source and destination for the next round
         */
        if(!mstStep.isEnd()) {
          mstStep.setSource(mstStep.getDestination());
          dest=new Graph(getSource());
          dest.setPath(tempDirs.getTempDir());
          mstStep.setDestination(dest);
        }
        else {
          /**
           * on algorithm's termination, trim the unprocessed messages off,
           * only keep the vertexes information
           */
          mstStep.setSource(mstStep.getDestination());
          dest=new Graph(getSource());
          dest.setPath(new Path(finalOutputFile()));
          mstStep.setDestination(dest);
          mstStep.execute();
        }
      }
      /**
       * Close the distributed file system
       */
      client.close();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}

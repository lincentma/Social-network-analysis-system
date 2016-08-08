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
package org.sf.xrime.algorithms.pagerank;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.pagerank.normal.PageRankAlgorithm;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.utils.NetUtils;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * As we know, PageRank is an iterated algorithm. PageRankStep is an iteration.
 * PageRankStep also implement 
 * @author Cai Bin 
 */
public class PageRankStep extends GraphAlgorithm implements ZeroOutDegreeVertexRankCollector {  
  /**
   * Continue indicator file name in destination working directory
   */
  private String continueFileName="continue";  
  
  /**
   * Key in JobConf for continue indicator file name
   */
  static final public String continueFileKey  ="xrime.algorithm.PageRank.continue.flag";
  
  /**
   * Key in JobConf for JobTracker host name, which used to open a ZeroOutDegreeVertexRankCollector server
   */
  static final public String rankCollectorHost="xrime.algorithm.PageRank.collector.host";
  
  /**
   * Key in JobConf for JobTracker port, which used to open a ZeroOutDegreeVertexRankCollector server
   */
  static final public String rankCollectorPort="xrime.algorithm.PageRank.collector.port";
  
  /**
   * Whether more iteration is needed.
   */
  private boolean end=false;

  private JobConf jobConf;  
  private FileSystem client=null;
  
  private double stopThreshold=0.01;
  private double dampingFactor=1;
//  private long   vertexNumber=0;

  private long zeroOutDegreeVertexCount=0L;
  private double zeroOutDegreeVertexRank=0.0;
  private double zeroOutDegreeVertexRankPrevious=0.0;
  private Set<String> reported=new HashSet<String>();
  
  static final public long protocolVersion=5473L;
  private Server server;
  private InetAddress ipAddress=null;
  private int ipPort=0;

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
  
  public boolean isEnd() {
    return end;
  }  
  
  public FileSystem getClient() {
    return client;
  }

  public void setClient(FileSystem client) {
    this.client = client;
  }

  public void setStopThreshold(double stopThreshold) {
    this.stopThreshold = stopThreshold;
  }

  public void setDampingFactor(double dampingFactor) {
    this.dampingFactor = dampingFactor;
  }
//  
//  public void setVertexNumber(long vertexNumber) {
//    this.vertexNumber = vertexNumber;
//  }

  public long getZeroOutDegreeVertexCount() {
    return zeroOutDegreeVertexCount;
  }

  public double getZeroOutDegreeVertexRank() {
    return zeroOutDegreeVertexRank;
  }
  
  public void resetZeroOutDegreeVertexInfo() {
    synchronized(reported) {
      reported.clear();
      zeroOutDegreeVertexRank=0.0;
      zeroOutDegreeVertexCount=0L;
    }
  }

  public double getZeroOutDegreeVertexRankPrevious() {
    return zeroOutDegreeVertexRankPrevious;
  }

  public void setZeroOutDegreeVertexRankPrevious(
      double zeroOutDegreeVertexRankPrevious) {
    this.zeroOutDegreeVertexRankPrevious = zeroOutDegreeVertexRankPrevious;
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      if (server == null) {
        startServer(); // start the server
      }
      resetZeroOutDegreeVertexInfo(); // reset zeroOutDegreeVertexRank &
                                      // zeroOutDegreeVertexCount
      
      // {{ algorithm properties
      context.setParameter(continueFileKey, continueFlagFile());
      context.setParameter(rankCollectorHost, ipAddress.getHostName());
      context.setParameter(rankCollectorPort, Integer.toString(ipPort));
      context.setParameter(PageRankAlgorithm.pageRankDampingFactorKey, Double.toString(dampingFactor));
      context.setParameter(PageRankAlgorithm.pageRankStopThresholdKey, Double.toString(stopThreshold));
      // }} algorithm properties
      // {{ Mapper, Reducer configuration
      jobConf = new JobConf(context, PageRankStep.class);
      jobConf.setJobName("BFS");

      FileInputFormat.setInputPaths(jobConf, context.getSource().getPath());

      jobConf.setInputFormat(SequenceFileInputFormat.class);
      jobConf.setMapperClass(PageRankMapper.class);
      jobConf.setNumMapTasks(getMapperNum());
      jobConf.setMapOutputValueClass(ObjectWritable.class);

      jobConf.setReducerClass(PageRankReducer.class);
      jobConf.setNumReduceTasks(getReducerNum());

      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(LabeledAdjSetVertex.class);

      FileOutputFormat.setOutputPath(jobConf, context.getDestination().getPath());
      jobConf.setOutputFormat(SequenceFileOutputFormat.class);
      // }} Mapper, Reducer configuration

      this.runningJob = JobClient.runJob(jobConf);

      // {{ end indicator
      if (client == null) {
        client = FileSystem.get(jobConf);
      }

      if (client.exists(new Path(continueFlagFile()))) {
        end = false;
        client.delete(new Path(continueFlagFile()), true);
      } else {
        end = true;
      }
      // }} end indicator

      // {{ correction
      if (zeroOutDegreeVertexRank > 0) {
        SequenceTempDirMgr dirmgr = new SequenceTempDirMgr(context
            .getDestination().getPath().toString());
        Path tmpFile = dirmgr.getTempDir();
        
        // prop.setProperty(PageRankAlgorithm.pageRankVertexAdjKey,
        // Double.toString(dampingFactor*zeroOutDegreeVertexRank/vertexNumber));
        context.setParameter(PageRankAlgorithm.pageRankVertexAdjKey, 
            Double.toString(dampingFactor * zeroOutDegreeVertexRank));
        System.out.println("dampingFactor*zeroOutDegreeVertexRank: "
            + dampingFactor * zeroOutDegreeVertexRank);
        // {{ Mapper, Reducer configuration
        jobConf = new JobConf(context, PageRankStep.class);
        jobConf.setJobName("BFS");

        FileInputFormat.setInputPaths(jobConf, context.getDestination()
            .getPath());

        jobConf.setInputFormat(SequenceFileInputFormat.class);
        jobConf.setMapperClass(PageRankCorrectionMapper.class);
        jobConf.setNumMapTasks(getMapperNum());
        jobConf.setNumReduceTasks(0); // no reducer here

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LabeledAdjSetVertex.class);

        FileOutputFormat.setOutputPath(jobConf, tmpFile);
        jobConf.setOutputFormat(SequenceFileOutputFormat.class);
        // }} Mapper, Reducer configuration

        this.runningJob = JobClient.runJob(jobConf);

        // {{End? We need additional check here since PageRankCorrectionMapper
        // will generate new PR value.
        if (client.exists(new Path(continueFlagFile()))) {
          end = false;
          client.delete(new Path(continueFlagFile()), true);
        } else {
          end = true;
        }
        // }}End? We need additional check here since PageRankCorrectionMapper
        // will generate new PR value.

        client.delete(context.getDestination().getPath(), true);
        client.rename(tmpFile, context.getDestination().getPath());
      }
      // }} correction
    } catch (IOException e) {
      end = true;
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }

  public synchronized void startServer() throws IOException {
    if(server!=null) {
      return;
    }
    
    ipAddress=NetUtils.getLocalIP();
    ipPort=NetUtils.getFreePort();
    server = RPC.getServer(this, ipAddress.getHostName(), ipPort, context);
    server.start();
  }
  
  public void stopServer() {
    server.stop();
  }
  
  @Override
  public void postRank(String tanskID, long count, double rank) {
    synchronized(reported) {
      if(!reported.contains(tanskID)) {
            zeroOutDegreeVertexRank+=rank;
            zeroOutDegreeVertexCount+=count;
            reported.add(tanskID);
      }
    }
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return protocolVersion;
  }
}

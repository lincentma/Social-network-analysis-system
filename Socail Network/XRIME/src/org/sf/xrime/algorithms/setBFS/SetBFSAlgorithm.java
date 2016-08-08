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
package org.sf.xrime.algorithms.setBFS;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * Set BFS visits the graph that begins from a node set and explores all the neighboring nodes.
 * @see org.sf.xrime.algorithms.BFS.BFSAlgorithm
 */
public class SetBFSAlgorithm extends GraphAlgorithm {
  static final public String setBFSInitNodePath  = "init";

  private JobConf jobConf;
  private Set<LabeledAdjSetVertex> initVertexs=null;
  private FileSystem client=null;  
  private SequenceTempDirMgr tempDirs=null;
  
  /**
   * Add a vertex to node set.
   * @param bfsVertex vertex
   */
  public void addInitVertex(LabeledAdjSetVertex bfsVertex) {
    addVertex(bfsVertex);;
  }
  
  /**
   * Add a vertex to node set.
   * @param bfsVertex vertex id
   */
  public void addInitVertex(String vertexId) {
    LabeledAdjSetVertex bfsVertex=new LabeledAdjSetVertex();
    bfsVertex.setId(vertexId);
    addVertex(bfsVertex);;
  }
  
  private void addVertex(LabeledAdjSetVertex bfsVertex) {
    if(initVertexs==null) {
      initVertexs=new HashSet<LabeledAdjSetVertex>();
    }
    initVertexs.add(bfsVertex);
  }

  /**
   * Clear the node set.
   */
  public void clearInitVertex() {
    initVertexs.clear();
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
      jobConf = new JobConf(context, SetBFSAlgorithm.class);
    
      client=FileSystem.get(jobConf);
      
      if(tempDirs==null) {
        tempDirs=new SequenceTempDirMgr(context.getDestination().getPath());
      }
      tempDirs.setFileSystem(client);
      
      insertInitNode(setBFSInitNodePath);
      
      SetBFSStep bfsStep=new SetBFSStep();
      bfsStep.getContext().setSource(getSource());
      Graph dest=new Graph(getSource());
      dest.setPath(tempDirs.getTempDir());
      bfsStep.getContext().setDestination(dest);
      
      while(true) {
        bfsStep.execute();
        if( bfsStep.isEnd() ) {
          break;
        }            

        bfsStep.setSource(bfsStep.getContext().getDestination());
        dest=new Graph(getSource());
        dest.setPath(tempDirs.getTempDir());
        bfsStep.getContext().setDestination(dest);
      }
        
      client.close();
      
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }
  
  /**
   * Insert initial vertexes. We will store this vertexes in a SequenceFile.  
   * @param fileName SequenceFile name to store the vertexes.
   * @throws IOException
   */
  private void insertInitNode(String fileName) throws IOException {
    if(initVertexs!=null) {        
      // get the path of the Init output file
      Path filePath=new Path(context.getSource().getPaths().get(0).toString()+"/"+fileName);
        
      Path path = new Path(jobConf.getWorkingDirectory(), filePath);
      FileSystem fs = path.getFileSystem(jobConf);
      CompressionCodec codec = null;
      CompressionType compressionType = CompressionType.NONE;
      if (jobConf.getBoolean("mapred.output.compress", false)) {
        // find the kind of compression to do            
        String val = jobConf.get("mapred.output.compression.type", CompressionType.RECORD.toString());
        compressionType=CompressionType.valueOf(val);

        // find the right codec
        Class<? extends CompressionCodec> codecClass = DefaultCodec.class;
            
        String name = jobConf.get("mapred.output.compression.codec");
        if (name != null) {
          try {
            codecClass = jobConf.getClassByName(name).asSubclass(CompressionCodec.class);
          } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Compression codec " + name + 
                " was not found.", e);
          }
        }
        codec = ReflectionUtils.newInstance(codecClass, jobConf);
      }
          
      SequenceFile.Writer out = SequenceFile.createWriter(fs, jobConf, path,
                                                  Text.class,
                                                  LabeledAdjSetVertex.class,
                                                  compressionType,
                                                  codec,
                                                  null);
                
      for(LabeledAdjSetVertex initVertex: initVertexs) {
        SetBFSLabel nowLabel  = (SetBFSLabel) initVertex.getLabel(SetBFSLabel.setBFSLabelPathsKey);
            
        if(nowLabel==null) {
          nowLabel=new SetBFSLabel();
        }
            
        nowLabel.setStatus(-1);
        nowLabel.setDistance(0);
        initVertex.setLabel(SetBFSLabel.setBFSLabelPathsKey, nowLabel);
            
        out.append(new Text(initVertex.getId()), initVertex);
      }
        
      out.close();
    }
  }
}

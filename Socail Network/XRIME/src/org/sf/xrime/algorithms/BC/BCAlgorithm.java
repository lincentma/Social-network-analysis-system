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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.SequenceFile.Reader;

import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;
import org.sf.xrime.utils.SequenceTempDirMgr;

/*
 * @author  Yang Cheng
 * this program indicates how the whole BC algorithms runs
 * the whole program includes two main step the forward step and the backward step
 * each of them may executed for several times
 * the times can not be predicted in advance, it depends on the farthest vertexes  from the source vertex, which is recorded in the 
 * variable distance
 */

public class BCAlgorithm extends GraphAlgorithm {
  static final public String bcInitNodePath  = "init";
  private JobConf jobConf;
  private LabeledAdjBiSetVertex initVertex=null;
  private FileSystem client=null;  
  private SequenceTempDirMgr tempDirs=null;
  private String vertex=null;
  private float BCvertex=0;
  
  public BCAlgorithm(String v)
  {
    vertex=v;
  }
  
  public void setBCvertex(float BCvertex)
  {
    this.BCvertex=BCvertex;
  }
  
  public float getBCvertex()
  {
    return BCvertex;
  }

  public void addInitVertex(LabeledAdjBiSetVertex bcVertex) {
    this.initVertex=bcVertex;
  }

  public void addInitVertex(String vertexId) {
    this.initVertex=new LabeledAdjBiSetVertex();
    initVertex.setId(vertexId);
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
      jobConf = new JobConf(context, BCAlgorithm.class);
      client=FileSystem.get(jobConf);

      if(tempDirs==null) {
        tempDirs=new SequenceTempDirMgr(context.getDestination().getPath());
      }
      tempDirs.setFileSystem(client);

      insertInitNode(bcInitNodePath);
      
      BCForwardStep bcStep=new BCForwardStep();
      bcStep.getContext().setSource(getSource());
      Graph dest=new Graph(getSource());
      dest.setPath(tempDirs.getTempDir());
      bcStep.getContext().setDestination(dest);
      int distance=0;                                                   //record the farthest vertex from the source  vertex
      while(true) {
        bcStep.execute();
        distance++;             //record the times that the forward 
        if( bcStep.isEnd() ) {
          break;
        }            
        bcStep.setSource(bcStep.getContext().getDestination());
        dest=new Graph(getSource());
        dest.setPath(tempDirs.getTempDir());
        bcStep.getContext().setDestination(dest);
      }

      BCBackwardStep bcstep2=new BCBackwardStep();
      bcstep2.getContext().setSource(bcStep.getContext().getDestination());
      Graph dest2=new Graph(context.getSource());
      dest2.setPath(tempDirs.getTempDir());
      bcstep2.getContext().setDestination(dest2);

      distance-=1; //
      while(distance>0)
      {  
        bcstep2.setDistance(distance);
        bcstep2.execute();
        distance--;
        bcstep2.setSource(bcstep2.getContext().getDestination());
        dest=new Graph(getSource());
        dest.setPath(tempDirs.getTempDir());
        bcstep2.getContext().setDestination(dest);         
      }
      bcstep2.setDistance(distance);               // one more step to deal with the source vertex 
      bcstep2.execute();
      
      Path filePath=bcstep2.getContext().getDestination().getPath();                            // the program's function in  the following lines is to read information from the reduce output 
      Path path = new Path(jobConf.getWorkingDirectory(), filePath+"/part-00000");
      FileSystem fs = FileSystem.get(jobConf);
      Reader reader=new Reader(fs,path,jobConf);
      Text key=new Text();
      LabeledAdjBiSetVertex  value=new LabeledAdjBiSetVertex();
      while(reader.next(key,value))                                             //get the LabeledAdjBiSetVertex which the id is the specified vertex
      {
        if(key.toString().equals(vertex))
        {
          BCLabel label=(BCLabel)(value.getLabel(BCLabel.bcLabelPathsKey));
          BCvertex=label.getBC();
          break;
        }
      }
      client.close();

    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }

  private void insertInitNode(String fileName) throws IOException {
    if(initVertex!=null) {
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
          LabeledAdjBiSetVertex.class,
          compressionType,
          codec,
          null);
      
      BCLabel nowLabel=new BCLabel();
      
      nowLabel.setStatus(-1);
    
      initVertex.setLabel(BCLabel.bcLabelPathsKey, nowLabel);
      
      out.append(new Text(initVertex.getId()), initVertex);
      out.close();
    }
  }
}

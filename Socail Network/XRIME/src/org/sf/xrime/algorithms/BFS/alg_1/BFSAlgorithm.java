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
package org.sf.xrime.algorithms.BFS.alg_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.sf.xrime.algorithms.BFS.BFSLabel;
import org.sf.xrime.algorithms.BFS.ExtractPathLength;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.LabeledAdjVertex;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * BFS visits the graph that begins at the root node and explores all the
 * neighboring nodes.
 * @see org.sf.xrime.algorithms.setBFS.SetBFSAlgorithm
 */
public class BFSAlgorithm extends GraphAlgorithm {
  static final public String bfsInitNodePath = "init";

  private JobConf jobConf;
  private LabeledAdjVertex initVertex = null;
  private FileSystem client = null;
  private SequenceTempDirMgr tempDirs = null;

  /**
   * Set the starting vertex.
   * @param bfsVertex vertex to start.
   */
  public void addInitVertex(LabeledAdjVertex bfsVertex) {
    this.initVertex = bfsVertex;
  }

  /**
   * Set the starting vertex.
   * @param vertexId vertex ID to start.
   */
  public void addInitVertex(String vertexId) {
    this.initVertex = new LabeledAdjVertex();
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
    Graph src = context.getSource();
    if (src == null) {
      return null;
    }

    try {
      return src.getPath();
    } catch (IllegalAccessException e) {
      return null;
    }
  }

  public void setSrcPath(Path srcPath) {
    Graph src = context.getSource();
    if (src == null) {
      src = new Graph(Graph.defaultGraph());
      context.setSource(src);
    }
    src.setPath(srcPath);
  }

  public Path getDestPath() {
    Graph dest = context.getDestination();
    if (dest == null) {
      return null;
    }

    try {
      return dest.getPath();
    } catch (IllegalAccessException e) {
      return null;
    }
  }

  public void setDestPath(Path destPath) {
    Graph dest = context.getDestination();
    if (dest == null) {
      dest = new Graph(Graph.defaultGraph());
      context.setDestination(dest);
    }
    dest.setPath(destPath);
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      jobConf = new JobConf(context, BFSAlgorithm.class);
      client = FileSystem.get(jobConf);

      if (tempDirs == null) {
        tempDirs = new SequenceTempDirMgr(context.getDestination().getPath().toString() + "/");
      }
      tempDirs.setFileSystem(client);
      tempDirs.setSeqNum(0);

      insertInitNode(bfsInitNodePath);

      BFSStep bfsStep = new BFSStep();
      bfsStep.getContext().setSource(getSource());
      Graph dest = new Graph(getSource());
      dest.setPath(tempDirs.getTempDir());
      bfsStep.getContext().setDestination(dest);
      bfsStep.setMapperNum(getMapperNum());
      bfsStep.setReducerNum(getReducerNum());

      while (true) {
        System.out.println("++++++>" + tempDirs.getSeqNum() + ": BFSStep.");
        bfsStep.execute();
        if (bfsStep.isEnd()) {
          break;
        }
        bfsStep.setSource(bfsStep.getContext().getDestination());
        dest = new Graph(getSource());
        dest.setPath(tempDirs.getTempDir());
        bfsStep.getContext().setDestination(dest);
      }

      ExtractPathLength extract_pl = new ExtractPathLength();
      extract_pl.setConf(context);
      extract_pl.setSource(bfsStep.getDestination());
      dest = new Graph(getSource());
      dest.setPath(tempDirs.getTempDir());
      extract_pl.setDestination(dest);
      extract_pl.setMapperNum(getMapperNum());
      extract_pl.setReducerNum(getReducerNum());
      System.out.println("++++++>" + tempDirs.getSeqNum()+": ExtractPathLength.");
      extract_pl.execute();
      
      client.close();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }

  /**
   * Insert initial vertex. We will store this vertex in a SequenceFile.
   * 
   * @param fileName
   *            SequenceFile name to store the vertex.
   * @throws IOException
   */
  private void insertInitNode(String fileName) throws IOException {
    if (initVertex != null) {
      BFSLabel nowLabel = (BFSLabel) initVertex
          .getLabel(BFSLabel.bfsLabelPathsKey);

      if (nowLabel == null) {
        nowLabel = new BFSLabel();
      }

      nowLabel.setStatus(-1);
      initVertex.setLabel(BFSLabel.bfsLabelPathsKey, nowLabel);

      // get the path of the Init output file
      Path filePath = new Path(context.getSource().getPaths().get(0)
          .toString()
          + "/" + fileName);

      Path path = new Path(jobConf.getWorkingDirectory(), filePath);
      FileSystem fs = path.getFileSystem(jobConf);
      CompressionCodec codec = null;
      CompressionType compressionType = CompressionType.NONE;
      if (jobConf.getBoolean("mapred.output.compress", false)) {
        // find the kind of compression to do
        String val = jobConf.get("mapred.output.compression.type",
            CompressionType.RECORD.toString());
        compressionType = CompressionType.valueOf(val);

        // find the right codec
        Class<? extends CompressionCodec> codecClass = DefaultCodec.class;

        String name = jobConf.get("mapred.output.compression.codec");
        if (name != null) {
          try {
            codecClass = jobConf.getClassByName(name).asSubclass(
                CompressionCodec.class);
          } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Compression codec "
                + name + " was not found.", e);
          }
        }
        codec = ReflectionUtils.newInstance(codecClass, jobConf);
      }

      SequenceFile.Writer out = SequenceFile.createWriter(fs, jobConf,
          path, Text.class, LabeledAdjVertex.class, compressionType,
          codec, null);

      out.append(new Text(initVertex.getId()), initVertex);
      out.close();
    }
  }

  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    List<String> other_args = new ArrayList<String>();
    String init_vertex = null;
    for(int i=0; i < params.length; i++) {
      if ("-init".equals(params[i])) {
        // id of the starting vertex of BFS.
        init_vertex = params[++i];
        addInitVertex(init_vertex);
      } else {
        other_args.add(params[i]);
      }
    }
    
    if(init_vertex == null)
      throw new ProcessorExecutionException("You need to specify the starting vertex of BFS.");
        // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters left: " +
                         other_args.size() + " instead of 2.");
    }

    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(new Path(other_args.get(0)));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(other_args.get(1)));
    
    setSource(src);
    setDestination(dest);
  }
}

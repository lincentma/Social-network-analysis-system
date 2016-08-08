/*
 * Copyright (C) liuchangyan@BUPT. 2009.
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
package org.sf.xrime.algorithms.layout.radialtree;

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
import org.sf.xrime.Transformer;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.layout.ConstantLabels;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.LabeledAdjVertex;
import org.sf.xrime.postprocessing.SequenceFileToTextFileTransformer;
import org.sf.xrime.utils.SequenceTempDirMgr;

/**
 * In the radial tree layout a single node is placed at the center of 
 * the display and all the other nodes are laid around it.The entire 
 * graph is like a tree rooted at the central node. The central node 
 * is refered to as the focus node and all the other nodes are arranged 
 * on concentric rings around it. Each node lies on the ring corresponding 
 * to its shortest network distance from the focus. Any two nodes joined 
 * by an edge in the graph is refered to as neighbors. Immediate neighbors 
 * of the focus lie on the smallest inner ring, their neighbors lie on the 
 * second smallest ring, and so on.
 * @author liu chang yan
 */
public class RadialTreeAlgorithm extends GraphAlgorithm {
  static final public String RadialTreeInitNodePath  = "init";
  private JobConf jobConf;
  private LabeledAdjVertex initVertex=null;
  private FileSystem client=null;  
  private SequenceTempDirMgr tempDirs=null;
  
  public void addInitVertex(LabeledAdjVertex RadialTreeVertex) {
    this.initVertex=RadialTreeVertex;
  }
  
  public void addInitVertex(String vertexId) {
    this.initVertex=new LabeledAdjVertex();
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
      jobConf = new JobConf(context, RadialTreeAlgorithm.class);

      // Get the size of the display frame.
      int max_x = Integer.parseInt(context
          .getParameter(ConstantLabels.MAX_X_COORDINATE));
      int max_y = Integer.parseInt(context
          .getParameter(ConstantLabels.MAX_Y_COORDINATE));

      if (tempDirs == null) {
        tempDirs = new SequenceTempDirMgr(context.getDestination().getPath());
      }
      client = FileSystem.get(jobConf);
      tempDirs.setFileSystem(client);

      // Choose a node as the root node
      insertInitNode(RadialTreeInitNodePath);

      // Use to record the depth of the tree
      long counter1 = 0;
      long counter2 = 0;
      long counter3 = 0;

      // Step1: Get the topology of the tree
      System.out.println("------------step1------------");
      RadialTreeStep1 radialTreeStep1 = new RadialTreeStep1();
      radialTreeStep1.setConf(context);
      radialTreeStep1.setSource(getSource());
      Graph dest1 = new Graph(getSource());
      dest1.setPath(tempDirs.getTempDir());
      radialTreeStep1.setDestination(dest1);
      radialTreeStep1.setParameter(ConstantLabels.MAX_X_COORDINATE, "" + max_x);
      radialTreeStep1.setParameter(ConstantLabels.MAX_Y_COORDINATE, "" + max_y);

      while (true) {
        radialTreeStep1.execute();
        if (radialTreeStep1.isEnd()) {
          break;
        }
        counter1++;
        radialTreeStep1
            .setSource(radialTreeStep1.getContext().getDestination());
        dest1 = new Graph(getSource());
        dest1.setPath(tempDirs.getTempDir());
        radialTreeStep1.getContext().setDestination(dest1);
      }

      counter1--;
      counter2 = counter1;
      counter3 = counter1;

      // Step2: Caculate every subtree's weight
      System.out.println("------------step2------------");
      RadialTreeStep2 radialTreeStep2 = new RadialTreeStep2();
      radialTreeStep2.setConf(context);
      radialTreeStep2.setSource(radialTreeStep1.getDestination());
      Graph dest2 = new Graph(getSource());
      dest2.setPath(tempDirs.getTempDir());
      radialTreeStep2.setDestination(dest2);
      radialTreeStep2.setParameter(ConstantLabels.MAX_X_COORDINATE, "" + max_x);
      radialTreeStep2.setParameter(ConstantLabels.MAX_Y_COORDINATE, "" + max_y);
      while (true) {
        radialTreeStep2.setParameter(ConstantLabels.NUM_OF_VERTEXES, ""
            + counter2);
        counter2--;
        radialTreeStep2.execute();
        if (radialTreeStep2.isEnd()) {
          break;
        }
        radialTreeStep2
            .setSource(radialTreeStep2.getContext().getDestination());
        dest2 = new Graph(getSource());
        dest2.setPath(tempDirs.getTempDir());
        radialTreeStep2.getContext().setDestination(dest2);
      }

      // Caculate the increment of radius
      int radius = (max_x < max_y) ? max_x * 9 / 10 : max_y * 9 / 10;
      radius /= counter1;

      // Step3: Caculate every node's coordinates
      System.out.println("------------step3------------");
      RadialTreeStep3 radialTreeStep3 = new RadialTreeStep3();
      radialTreeStep3.setConf(context);
      radialTreeStep3.setSource(radialTreeStep2.getDestination());
      Graph dest3 = new Graph(getSource());
      dest3.setPath(tempDirs.getTempDir());
      radialTreeStep3.setDestination(dest3);
      radialTreeStep3.setParameter(ConstantLabels.MAX_X_COORDINATE, "" + max_x);
      radialTreeStep3.setParameter(ConstantLabels.MAX_Y_COORDINATE, "" + max_y);
      radialTreeStep3.setParameter(ConstantLabels.TEMPERATURE, "" + radius);
      while (counter3 >= -1) {
        radialTreeStep3.setParameter(ConstantLabels.NUM_OF_VERTEXES, ""
            + (counter1 - counter3));
        radialTreeStep3.setParameter(ConstantLabels.START_SEQ_NUM, ""
            + counter1);
        counter3--;
        radialTreeStep3.execute();
        radialTreeStep3
            .setSource(radialTreeStep3.getContext().getDestination());
        dest3 = new Graph(getSource());
        dest3.setPath(tempDirs.getTempDir());
        radialTreeStep3.getContext().setDestination(dest3);
      }

      // Step4: Translate result to text file in order to read and draw
      System.out.println("------------step4------------");
      Transformer textifier = new SequenceFileToTextFileTransformer();
      textifier.setConf(context);
      textifier.setSrcPath(radialTreeStep3.getContext().getSource().getPath());
      textifier.setDestPath(radialTreeStep3.getContext().getDestination()
          .getPath());
      textifier.setMapperNum(getMapperNum());
      textifier.setReducerNum(getReducerNum());
      textifier.execute();

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
   *          SequenceFile name to store the vertex.
   * @throws IOException
   */
  private void insertInitNode(String fileName) throws IOException {
    if (initVertex != null) {
      RadialTreeLabel nowLabel = (RadialTreeLabel) initVertex
          .getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);

      if (nowLabel == null) {
        nowLabel = new RadialTreeLabel();
      }

      nowLabel.setStatus(-1);
      initVertex.setLabel(RadialTreeLabel.RadialTreeLabelPathsKey, nowLabel);

      // get the path of the Init output file
      Path filePath = new Path(context.getSource().getPaths().get(0).toString()
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
            throw new IllegalArgumentException("Compression codec " + name
                + " was not found.", e);
          }
        }
        codec = ReflectionUtils.newInstance(codecClass, jobConf);
      }

      SequenceFile.Writer out = SequenceFile.createWriter(fs, jobConf, path,
          Text.class, LabeledAdjVertex.class, compressionType, codec, null);

      out.append(new Text(initVertex.getId()), initVertex);
      out.close();
    }
  }

  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    List<String> other_args = new ArrayList<String>();
    int max_x = 0;
    int max_y = 0;
    String id = new String();
    for (int i = 0; i < params.length; ++i) {
      try {
        if ("-x".equals(params[i])) {
          max_x = Integer.parseInt(params[++i]);
        } else if ("-y".equals(params[i])) {
          max_y = Integer.parseInt(params[++i]);
        } else if ("-id".equals(params[i])) {
          id = params[++i];
        } else {
          other_args.add(params[i]);
        }
      } catch (NumberFormatException except) {
        throw new ProcessorExecutionException("Integer expected instead of " + params[i]);
      } catch (ArrayIndexOutOfBoundsException except) {
        throw new ProcessorExecutionException("Required parameter missing from " + params[i - 1]);
      }
    }
    // Need to specify the size of frame.
    if(max_x == 0 || max_y == 0) {
      throw new ProcessorExecutionException("Should specify the size of display frame");
    }
    if(other_args.size()!=2){
      throw new ProcessorExecutionException("Need to specify input and output paths.");
    }
    setSrcPath(new Path(other_args.get(0)));
    setDestPath(new Path(other_args.get(1)));
    setParameter(ConstantLabels.MAX_X_COORDINATE, "" + max_x);
    setParameter(ConstantLabels.MAX_Y_COORDINATE, "" + max_y);
    // Add the root vertex
    addInitVertex(id);
  }
}

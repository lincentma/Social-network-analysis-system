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
package org.sf.xrime.algorithms.layout.ellipse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.layout.ConstantLabels;

import org.sf.xrime.algorithms.transform.vertex.OutAdjVertex2AdjSetVertexTransformer;
import org.sf.xrime.model.Graph;
import org.sf.xrime.postprocessing.SequenceFileToTextFileTransformer;
import org.sf.xrime.utils.MRConsoleReader;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * This program transforms input to AdjSetVertex, assign sequential numbers to
 * vertices, calculates vertices coordinates and transforms output to text.
 * @author liu chang yan
 */
public class EllipseLayoutAlgorithm extends GraphAlgorithm {
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    List<String> other_args = new ArrayList<String>();
    int max_x = 0;
    int max_y = 0;
    for (int i = 0; i < params.length; ++i) {
      try {
        if ("-x".equals(params[i])) {
          max_x = Integer.parseInt(params[++i]);
        } else if ("-y".equals(params[i])) {
          max_y = Integer.parseInt(params[++i]);
        } else {
          other_args.add(params[i]);
        }
      } catch (NumberFormatException except) {
        throw new ProcessorExecutionException(except);
      } catch (ArrayIndexOutOfBoundsException except) {
        throw new ProcessorExecutionException(except);
      }
    }
    // Need to specify the size of frame.
    if (max_x == 0 || max_y == 0) {
      throw new ProcessorExecutionException("Should specify the size of display frame");
    }

    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters: "
          + other_args.size() + " instead of 2.");
    }

    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(new Path(other_args.get(0)));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(other_args.get(1)));

    setSource(src);
    setDestination(dest);
    setParameter(ConstantLabels.MAX_X_COORDINATE, "" + max_x);
    setParameter(ConstantLabels.MAX_Y_COORDINATE, "" + max_y);
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      if (getSource().getPaths() == null || getSource().getPaths().size() == 0
          || getDestination().getPaths() == null
          || getDestination().getPaths().size() == 0)
        throw new ProcessorExecutionException(
            "No input and/or output paths specified.");

      // The prefix used by temp directories which store intermediate results of
      // each steps.
      String temp_dir_prefix = getDestination().getPath().getParent().toString()
                               + "/ela_" + getDestination().getPath().getName() + "_";

      // Create the temporary directory manager.
      SequenceTempDirMgr dirMgr = new SequenceTempDirMgr(temp_dir_prefix, context);
      // Sequence number begins with zero.
      dirMgr.setSeqNum(0);
      Path tmpDir;

      // Get the specified iteration times and size of the display frame.
      int max_x = Integer.parseInt(context.getParameter(ConstantLabels.MAX_X_COORDINATE));
      int max_y = Integer.parseInt(context.getParameter(ConstantLabels.MAX_Y_COORDINATE));

      // 1. Transform input from outgoing adjacency vertexes lists to
      // AdjSetVertex.
      System.out.println("++++++>" + dirMgr.getSeqNum()+ ": Transform input to AdjSetVertex");
      Transformer transformer = new OutAdjVertex2AdjSetVertexTransformer();
      transformer.setConf(context);
      transformer.setSrcPath(getSource().getPath());
      // Generate temporary directory.
      tmpDir = dirMgr.getTempDir();
      // And use it as the destination directory.
      transformer.setDestPath(tmpDir);
      transformer.setMapperNum(getMapperNum());
      transformer.setReducerNum(getReducerNum());
      transformer.execute();

      Graph src;
      Graph dest;

      // 2. Assign sequential numbers to vertexes.
      System.out.println("++++++>" + dirMgr.getSeqNum()+ ": SequentialNumAssign");
      src = new Graph(Graph.defaultGraph());
      src.setPath(tmpDir);
      tmpDir = dirMgr.getTempDir();
      dest = new Graph(Graph.defaultGraph());
      dest.setPath(tmpDir);
      GraphAlgorithm gen_seq_num = new SequentialNumAssign();
      gen_seq_num.setConf(context);
      gen_seq_num.setSource(src);
      gen_seq_num.setDestination(dest);
      gen_seq_num.setMapperNum(getMapperNum());
      gen_seq_num.setReducerNum(getReducerNum());
      gen_seq_num.execute();

      // Get the number of vertices.
      int num_of_vertexes = (int) MRConsoleReader.getReduceOutputRecordNum(gen_seq_num.getFinalStatus());

      System.out.println("" + max_x + " : " + max_y + " : " + num_of_vertexes);

      // 3. Determine coordinates for vertexes.
      System.out.println("++++++>" + dirMgr.getSeqNum()+ ": CoordinatesCalculation");
      src = new Graph(Graph.defaultGraph());
      src.setPath(tmpDir);
      tmpDir = dirMgr.getTempDir();
      dest = new Graph(Graph.defaultGraph());
      dest.setPath(tmpDir);
      GraphAlgorithm xy_calc = new CoordinatesCalculation();
      xy_calc.setConf(context);
      xy_calc.setParameter(ConstantLabels.MAX_X_COORDINATE, "" + max_x);
      xy_calc.setParameter(ConstantLabels.MAX_Y_COORDINATE, "" + max_y);
      xy_calc.setParameter(ConstantLabels.NUM_OF_VERTEXES, "" + num_of_vertexes);
      xy_calc.setSource(src);
      xy_calc.setDestination(dest);
      xy_calc.setMapperNum(getMapperNum());
      xy_calc.setReducerNum(getReducerNum());
      xy_calc.execute();
      // 4. Textify the result.
      System.out.println("++++++>" + dirMgr.getSeqNum()+ ": Textify the result");
      Transformer textifier = new SequenceFileToTextFileTransformer();
      textifier.setConf(context);
      textifier.setSrcPath(tmpDir);
      // And use it as the destination directory.
      textifier.setDestPath(getDestination().getPath());
      textifier.setMapperNum(getMapperNum());
      textifier.setReducerNum(getReducerNum());
      textifier.execute();
      
      // Delete all temporary directories.
      dirMgr.deleteAll();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }

  public static void main(String[] args) throws IOException {
    try {
      int res = ToolRunner.run(new EllipseLayoutAlgorithm(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

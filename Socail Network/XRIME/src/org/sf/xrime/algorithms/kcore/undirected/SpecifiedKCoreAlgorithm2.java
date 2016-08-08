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
package org.sf.xrime.algorithms.kcore.undirected;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.model.Graph;
import org.sf.xrime.postprocessing.SequenceFileToTextFileTransformer;
import org.sf.xrime.utils.MRConsoleReader;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * This class is used to calculate k-core with specified k. It wraps the recursive
 * invocation of ElementRemoval. We assume the input is in the form of AdjSetVertex.
 * So, this algorithm could be used to calculate a series of k-cores, like 0-core,
 * 1-core, 2-core, ...
 * @author xue
 */
public class SpecifiedKCoreAlgorithm2 extends GraphAlgorithm {
	/**
	 * Default constructor.
	 */
	public SpecifiedKCoreAlgorithm2(){
		super();
	}
	
	@Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    List<String> other_args = new ArrayList<String>();
    int the_k_value = -1;
    for (int i = 0; i < params.length; ++i) {
      try {
        if ("-k".equals(params[i])) {
          // Specify the k value.
          the_k_value = Integer.parseInt(params[++i]);
        } else {
          other_args.add(params[i]);
        }
      } catch (NumberFormatException except) {
        throw new ProcessorExecutionException(except);
      }
    }

    if (the_k_value < 0) {
      throw new ProcessorExecutionException(
          "You should specify the k value you are interested in.");
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters: "
          + other_args.size() + " instead of 2.");
    }

    // Pass the k value as a configuration parameter.
    setParameter(ElementRemoval.K_OF_CORE, "" + the_k_value);
    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(new Path(other_args.get(0)));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(other_args.get(1)));
    setSource(src);
    setDestination(dest);  }

  @Override
	public void execute() throws ProcessorExecutionException {
		try {
			if(getSource().getPaths()==null||getSource().getPaths().size()==0||
					getDestination().getPaths()==null||getDestination().getPaths().size()==0){
				throw new ProcessorExecutionException("No input and/or output paths specified.");
			}

			// Determine the specified k value.
			int the_k_value = Integer.parseInt(context.getParameter(ElementRemoval.K_OF_CORE));

			// The prefix used by temp directories which store intermediate results.
			String temp_dir_prefix = getDestination().getPath().getParent().toString()+"/"+
			                         the_k_value + "core_temp_";

			// Create the temporary directory manager.
			SequenceTempDirMgr dirMgr = new SequenceTempDirMgr(temp_dir_prefix, context);
			// Sequence number begins with zero.
			dirMgr.setSeqNum(0);
			
			// Set the initial path.
			Path tmpDir = getSource().getPath();
			// 2. Recursively invoke ElementRemoval to remove vertexes, and lines incident with
			// them, of degree less than k.
			long vertexes_num = -1;
			Graph src;
			Graph dest;
			while(true){
				// 2.1. Invoke ElementRemoval.
				System.out.println("-------->"+dirMgr.getSeqNum()+": Recursively delete vertexes and lines");
				GraphAlgorithm element_rm = new ElementRemoval();
				element_rm.setConf(context);
				src = new Graph(Graph.defaultGraph());
				// Use the output directory of last step as the input directory of this step.
				src.setPath(tmpDir);
				dest = new Graph(Graph.defaultGraph());
				// Generate a new temporary directory.
				tmpDir = dirMgr.getTempDir();
				dest.setPath(tmpDir);
				element_rm.setSource(src);
				element_rm.setDestination(dest);
				element_rm.setMapperNum(getMapperNum());
				element_rm.setReducerNum(getReducerNum());
				// Specify the K we are interested in.
				element_rm.setParameter(ElementRemoval.K_OF_CORE, 
				    context.getParameter(ElementRemoval.K_OF_CORE));
				element_rm.execute();

				// 2.2. Check for convergence.
				RunningJob conv_result = element_rm.getFinalStatus();
				long found_vertexes_num = MRConsoleReader.getReduceOutputRecordNum(conv_result);
				// Check whether the number of remaining vertexes changes.
				if(found_vertexes_num!=vertexes_num){
					// Changed! We need another iteration.
					vertexes_num = found_vertexes_num;
				}else{
					// Converged!
					break;
				}
			}

			// 3. Textify the result.
			System.out.println("-------->"+dirMgr.getSeqNum()+": Textify the result");
			Transformer transformer = new SequenceFileToTextFileTransformer();
			transformer.setConf(context);
			transformer.setSrcPath(tmpDir);
			transformer.setDestPath(getDestination().getPath());
			transformer.setMapperNum(getMapperNum());
			transformer.setReducerNum(getReducerNum());
			transformer.execute();
			
			// Delete all temporary directories.
			dirMgr.deleteAll();
		} catch (IllegalAccessException e) {
			throw new ProcessorExecutionException(e);
		} catch (IOException e) {
			throw new ProcessorExecutionException(e);
		} catch (NumberFormatException e){
			throw new ProcessorExecutionException(e);
		}
	}

	public static void main(String[] args){
	  try {
      int res = ToolRunner.run(new SpecifiedKCoreAlgorithm2(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}

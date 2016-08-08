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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.transform.vertex.OutAdjVertex2AdjSetVertexTransformer;
import org.sf.xrime.model.Graph;
import org.sf.xrime.postprocessing.SequenceFileToTextFileTransformer;
import org.sf.xrime.utils.MRConsoleReader;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * This class is used to calculate all k-cores of a graph. We assume the input is in
 * the form of outgoing adjacency vertexes lists.
 * @author xue
 */
public class AllKCoreAlgorithm extends GraphAlgorithm {
	/**
	 * Default constructor.
	 */
	public AllKCoreAlgorithm(){
		super();
	}
	
	@Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
		// Make sure there are exactly 2 parameters left.
		if (params.length != 2) {
			throw new ProcessorExecutionException("Wrong number of parameters: " +
					params.length + " instead of 2.");
		}

		// Configure the algorithm instance.
		Graph src = new Graph(Graph.defaultGraph());
		src.setPath(new Path(params[0]));
		Graph dest = new Graph(Graph.defaultGraph());
		dest.setPath(new Path(params[1]));
		setSource(src);
		setDestination(dest);
  }


  @Override
	public void execute() throws ProcessorExecutionException {
		try {
			if(getSource().getPaths()==null||getSource().getPaths().size()==0||
					getDestination().getPaths()==null||getDestination().getPaths().size()==0){
				throw new ProcessorExecutionException("No input and/or output paths specified.");
			}

			// The prefix used by temp directories which store intermediate results of each steps.
			String temp_dir_prefix = getDestination().getPath().toString()+"/kcore_";

			// Create the temporary directory manager.
			SequenceTempDirMgr dirMgr = new SequenceTempDirMgr(temp_dir_prefix, context);
			// Sequence number begins with zero.
			dirMgr.setSeqNum(0);
			Path tmpDir;

			// 1. Transform input from outgoing adjacency vertexes lists to AdjSetVertex.
			System.out.println("-------->"+dirMgr.getSeqNum()+": Transform input to AdjSetVertex");
			Transformer transformer = new OutAdjVertex2AdjSetVertexTransformer();
			// Inherit settings from this algorithm.
			transformer.setConf(context);
			transformer.setSrcPath(getSource().getPath());
			// Generate temporary directory.
			tmpDir = dirMgr.getTempDir();
			// And use it as the destination directory.
			transformer.setDestPath(tmpDir);
			transformer.setMapperNum(getMapperNum());
			transformer.setReducerNum(getReducerNum());
			transformer.execute();
			

			// 2. Recursively invoke ElementRemoval to remove vertexes, and lines incident with
			// them, of degree less than k. And, loop to find all possible k.
			Graph src;
			Graph dest;
			
			// The current k value to generate k-core for.
			int the_k_value = 1;
			// The number of vertexes in current k-core.
			long vertexes_num = -1;
			// Get the file system client object.
			FileSystem fs_client = FileSystem.get(context);
			while(vertexes_num!=0){
			  // If we still have k-core with larger k.
	      while(true){
	        // 2.1. Invoke ElementRemoval.
	        System.out.println("-------->"+dirMgr.getSeqNum()+": Recursively delete vertexes and lines");
	        GraphAlgorithm element_rm = new ElementRemoval();
	        // Inherit settings from this algorithm.
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
	        element_rm.setParameter(ElementRemoval.K_OF_CORE, ""+the_k_value);
	        element_rm.execute();

	        // 2.2. Check for convergence.
	        RunningJob conv_result = element_rm.getFinalStatus();
	        long found_vertexes_num = MRConsoleReader.getReduceOutputRecordNum(conv_result);
	        
	        // Check whether the number of remaining vertexes changes.
	        if(found_vertexes_num!=vertexes_num){
	          // Changed! We need another iteration.
	          vertexes_num = found_vertexes_num;
	        }else{
	          // Converged! Record it by rename the directory.
	          System.out.println("--------> Determined " + the_k_value + "-core");
	          // Determine the destination path.
	          Path dest_path = new Path(getDestination().getPath().toString()+"/"+the_k_value+"-core");
	          // Rename it.
	          fs_client.rename(tmpDir, dest_path);
	          // Remember this.
	          tmpDir = dest_path;
	          
	          // Textify this core.
	          System.out.println("--------> Textifying " + the_k_value + "-core");
	          transformer = new SequenceFileToTextFileTransformer();
	          // Inherit settings from this algorithm.
	          transformer.setConf(context);
	          transformer.setSrcPath(dest_path);
	          // Generate a directory for textify purpose.
	          Path dest_txt_path = new Path(getDestination().getPath().toString()+"/"+the_k_value+"-core-txt");
	          // And use it as the destination directory.
	          transformer.setDestPath(dest_txt_path);
	          transformer.setMapperNum(getMapperNum());
	          transformer.setReducerNum(getReducerNum());
	          transformer.execute();

	          // Increase the k value.
	          the_k_value++;
	          break;
	        }
	      }
			}
			// Delete all useless temporary directories.
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
      int res = ToolRunner.run(new AllKCoreAlgorithm(), args);
      System.exit(res);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
	}
}

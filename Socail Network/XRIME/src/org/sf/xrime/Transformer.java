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
package org.sf.xrime;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;

/**
 * The abstract base class for all transformers used in preprocessing or postprocessing
 * phases.
 */
public abstract class Transformer implements Processor, Tool {
	/**
	 * The path to input directory or file.
	 */
	protected Path srcPath;
	
	/**
	 * The path to output directory.
	 */
	protected Path destPath;
	
	/**
	 * Number of mappers. Default is 4.
	 */
	protected int mapperNum = 4;
	
	/**
	 * Number of reducers. Default is 4.
	 */
	protected int reducerNum = 4;

	/**
	 * Configuration of this Transformer
	 */
	protected Configuration conf;
	
	/**
	 * The job resulting status of this transformer.
	 */
	protected RunningJob runningJob;

	/**
	 * Default constructor.
	 */
	public Transformer(){
		conf = new Configuration();
	}

	/**
	 * Constructor.
	 * @param path_in
	 * @param path_out
	 */
	public Transformer(Path path_in, Path path_out){
		this();
		srcPath = path_in;
		destPath = path_out;
	}

	/** getter/setter */
	public Path getSrcPath() {
		return srcPath;
	}
	
	public void setSrcPath(Path srcPath) {
		this.srcPath = srcPath;
	}
	
	public Path getDestPath() {
		return destPath;
	}

	public void setDestPath(Path destPath) {
		this.destPath = destPath;
	}

	public int getMapperNum(){
		return mapperNum;
	}

	public void setMapperNum(int num){
		mapperNum = num;
	}

	public int getReducerNum(){
		return reducerNum;
	}

	public void setReducerNum(int num){
		reducerNum = num;
	}
	
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = new Configuration(conf);
	}
	
	/**
	 * Get the final status of this transformer.
	 * @return
	 */
	public RunningJob getFinalStatus(){
	  return runningJob;
	}
	
	/**
	 * {@inheritDoc}
	 * <p>Most transformers only take two processor specific parameters, namely, the input
	 * path and the output path. But exceptions do exist, in which case the developer should
	 * override this method.</p>
	 */
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    // Make sure there are exactly 2 parameters left.
    if (params.length != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters: " +
                         params.length + " instead of 2.");
    }
    setSrcPath(new Path(params[0]));
    setDestPath(new Path(params[1]));
  }

  @Override
  public int run(String[] args) throws Exception {
    List<String> other_args = new ArrayList<String>();
    // Deal with mapper and reducer numbers.
    try{
      for(int i=0; i < args.length; ++i) {
        if ("-m".equals(args[i])) {
          setMapperNum(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          setReducerNum(Integer.parseInt(args[++i]));
        /**
        } else if ("-vmr".equals(args[i])) {
          conf.setBoolean("mapred.versioned.enabled", true);
        } else if ("-parents".equals(args[i])){
          conf.set("mapred.versioned.parents", args[++i]);
        */
        } else {
          other_args.add(args[i]);
        }
      }
    }catch(Exception except){
      // Wrap and re-throw.
      throw new ProcessorExecutionException(except);
    }
    // Deal with processor specific parameters.
    setArguments(other_args.toArray(new String[2]));
    // Execute the processor.
    execute();
    return 0;
  }
}

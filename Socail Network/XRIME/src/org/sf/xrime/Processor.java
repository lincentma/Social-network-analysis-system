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


/**
 * The interface of a processor complying with our code framework.
 */
public interface Processor {
  /**
   * Execute the processor.
   * @throws ProcessorExecutionException
   */
	public void execute() throws ProcessorExecutionException;
	/**
	 * Set processor specific parameters from command line which start the execution of processor.
	 * Only overrided when the processor needs to deal with command line directly.
	 * @param params parameters from command line.
	 */
	public void setArguments(String[] params) throws ProcessorExecutionException;
}

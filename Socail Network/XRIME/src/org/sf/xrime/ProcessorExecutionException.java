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
 * An exception which is thrown when an error occurs in the process of executing
 * an {@link Processor}
 */
public class ProcessorExecutionException extends Exception {
	private static final long serialVersionUID = 90172770082L;

	public ProcessorExecutionException(String message, Throwable exception) {
		super(message, exception);
	}
	
	public ProcessorExecutionException(Throwable exception) {
		super(exception);
	}
	
	public ProcessorExecutionException(String message) {
		super(message);
	}
}

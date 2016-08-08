/*
 * Copyright (C) yangyin@BUPT. 2009.
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
package org.sf.xrime.preprocessing.pajek;

/**
 * 
 * @author yangyin
 * This Exception will be thrown when the input Pajek file format is not right
 */
public class WrongPajekFileFormatException extends Exception {
	/**  */
  private static final long serialVersionUID = 1L;

  public WrongPajekFileFormatException(String message, Throwable exception) {
		super(message, exception);
	}
	
	public WrongPajekFileFormatException(Throwable exception) {
		super(exception);
	}
	
	public WrongPajekFileFormatException(String message) {
		super(message);
	}
}

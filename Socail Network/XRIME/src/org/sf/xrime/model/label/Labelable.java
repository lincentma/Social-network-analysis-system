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
package org.sf.xrime.model.label;

import org.apache.hadoop.io.Writable;

/**
 * Class implements interface Labelable means could add addition information to
 * the object as <key, value>.
 * 
 * @author Cai Bin
 */
public interface Labelable {
	/**
	 * @param name label name
	 * @return label value
	 */
	public Writable getLabel(String name);
	
	/**
	 * @param name label name 
	 * @param value label value
	 */
	public void setLabel(String name, Writable value);

	/**
	 * Delete the label.
	 * @param name label name
	 */
	public void removeLabel(String name);
	
	/**
	 * Clear all labels.
	 */
	public void clearLabels();
}

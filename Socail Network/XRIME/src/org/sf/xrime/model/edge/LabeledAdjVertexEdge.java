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
package org.sf.xrime.model.edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.label.Labelable;
import org.sf.xrime.model.label.Labels;


/**
 * AdjVertexEdge with labels.
 */
public class LabeledAdjVertexEdge extends AdjVertexEdge implements Labelable {
  /**
   * Internal container of labels.
   */
	private Labels _labels;
	
	static {
	  // Register the writable factory of this class.
		WritableFactories.setFactory
		      (LabeledAdjVertexEdge.class,
		       new WritableFactory() {
		           public Writable newInstance() { return new LabeledAdjVertexEdge(); }
		       });
    }
	/**
	 * Default constructor.
	 */
	public LabeledAdjVertexEdge() {
	  super();
	  _labels = new Labels();
	}
	/**
	 * Another constructor.
	 * @param opposite
	 */
	public LabeledAdjVertexEdge(String opposite) {
	  super(opposite);
	  _labels = new Labels();
	}
	/**
	 * Yet another constructor.
	 * @param adjVertexEdge
	 */
	public LabeledAdjVertexEdge(AdjVertexEdge adjVertexEdge) {
	  super(adjVertexEdge);
	  _labels = new Labels();
	}
	/**
	 * Copy constructor.
	 */
	public LabeledAdjVertexEdge(LabeledAdjVertexEdge vertexEdge) {
	  super(vertexEdge);
	  _labels = (Labels) vertexEdge._labels.clone();
	}
	/**
	 * Get labels of this edge.
	 * @return
	 */
	public Labels getLabels() {
		return _labels;
	}
	/**
	 * Replace labels of this edge with the specified one.
	 * @param _labels
	 */
	public void setLabels(Labels _labels) {
		this._labels = _labels;
	}
	
	@Override
	public Writable getLabel(String name) {
		return _labels.getLabel(name);
	}

	@Override
	public void setLabel(String name, Writable value) {
		_labels.setLabel(name, value);
	}

	@Override
	public void removeLabel(String name) {
		_labels.removeLabel(name);
	}

	/**
	 * Get the string-valued label with specified name.
	 * @param name
	 * @return
	 */
	public String getStringLabel(String name) {
		return _labels.getStringLabel(name);
	}

	/**
	 * Add or modify a label with string value.
	 * @param name
	 * @param value
	 */
	public void setStringLabel(String name, String value) {
		_labels.setStringLabel(name, value);
	}

  @Override
  public void clearLabels() {
    _labels.clearLabels();
  }
    
	public String toString() {
		return "<" + opposite + ", <"+_labels+">>";
	}
    
	public Object clone() {
		return new LabeledAdjVertexEdge(this);
	}
	
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		_labels.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		_labels.write(out);
	}
}

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
 * Directed edge with labels.
 */
public class LabeledEdge extends Edge implements Labelable {
  /**
   * Internal container of labels.
   */
	private Labels _labels;

	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory
		      (LabeledEdge.class,
		       new WritableFactory() {
		           public Writable newInstance() { return new LabeledEdge(); }
		       });
    }
	/**
	 * Default constructor.
	 */
	public LabeledEdge() {
		super();
		_labels = new Labels();
	}
	/**
	 * Normal constructor.
	 * @param from
	 * @param to
	 */
	public LabeledEdge(String from, String to){
	  super(from, to);
	  _labels = new Labels();
	}
	/**
	 * Construct a labelable edge from specified Edge object.
	 * @param edge
	 */
	public LabeledEdge(Edge edge) {
		super(edge);
		_labels = new Labels();
	}
	/**
	 * Copy constructor.
	 * @param edge
	 */
	public LabeledEdge(LabeledEdge edge) {
		super(edge);
		_labels=(Labels) edge._labels.clone();
	}	
	/**
	 * Get the labels of this edge.
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
	 * Get the string valued label with specified name.
	 * 
	 * @param name
	 * @return
	 */
	public String getStringLabel(String name) {
		return _labels.getStringLabel(name);
	}

	/**
	 * Add or modify a label with string value.
	 * 
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
    
  @Override
	public String toString() {
    // Don't play with me.
		return "<" + super.toString() + ", " + _labels + ">";
	}
  
  @Override
  public void fromString(String encoding){
    // Clean.
    from = null;
    to = null;
    _labels.clearLabels();
    
    // Get edge part and labels part.
    int pointerA = 0, pointerB = 0;
    pointerB = encoding.indexOf(">, <", pointerA);
    String edge_str = encoding.substring(1, pointerB+1);
    String labels_str = encoding.substring(pointerB+3, encoding.length()-1);
    
    super.fromString(edge_str);
    _labels.fromString(labels_str);
  }

	public Object clone() {
		return new LabeledEdge(this);
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

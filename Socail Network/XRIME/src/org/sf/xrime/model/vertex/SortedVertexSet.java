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
package org.sf.xrime.model.vertex;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.Element;


/**
 * A set of vertexes.
 * @author xue
 */
public class SortedVertexSet extends VertexSet {
	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory(SortedVertexSet.class, new WritableFactory() {
			public Writable newInstance() {
				return new SortedVertexSet();
			}
		});
	}

	/**
	 * Basic constructor.
	 */
	public SortedVertexSet() {
		_vertexes = new TreeSet<Vertex>(new VertexComparator());
	}

	/**
	 * Copy constructor.
	 * 
	 * @param set
	 */
	public SortedVertexSet(SortedVertexSet set) {
		_vertexes = new TreeSet<Vertex>(new VertexComparator());
		for (Vertex vertex : set._vertexes) {
			_vertexes.add(new Vertex(vertex));
		}
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new SortedVertexSet(this);
	}

	/* 
	 * Since we do not really own any incident element, so hasNext() is always return false.
	 * @see org.sf.xrime.model.Element#getIncidentElements()
	 */
	@Override
	public Iterator<Element> getIncidentElements() {		
	  /**
	   * An internal class used to implement customized iterator logic.
	   */
		class Itr implements Iterator<Element> {
			@Override
			public boolean hasNext() {
				// we own nothing.
				return false;
			}

			@Override
			public Element next() {
				return null;
			}

			@Override
			public void remove() {		
				throw new UnsupportedOperationException("This is a read-only iterator");
			}			
		}
		
		return new Itr();
	}
}

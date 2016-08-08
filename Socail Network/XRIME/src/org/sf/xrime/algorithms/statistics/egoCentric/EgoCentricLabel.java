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
package org.sf.xrime.algorithms.statistics.egoCentric;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.sf.xrime.model.edge.Edge;

public class EgoCentricLabel implements Cloneable, Writable {
	List<Edge> edges;

	public EgoCentricLabel() {
	}

	public EgoCentricLabel(EgoCentricLabel label) {
		edges = new ArrayList<Edge>(label.getEdges());
	}

	public List<Edge> getEdges() {
		return edges;
	}

	public void setEdges(List<Edge> edges) {
		this.edges = edges;
	}

	public void addEdge(Edge edge) {
		if (edges == null) {
			edges = new ArrayList<Edge>();
		}

		edges.add(edge);
	}

	public void addEdges(List<Edge> edges) {
		if (edges == null) {
			edges = new ArrayList<Edge>();
		}

		edges.addAll(edges);
	}

	public int getEdgesCount() {
		if (edges == null) {
			return 0;
		}

		return edges.size();
	}

	public String toString() {
		if(edges==null) {
			return "<>";
		}
		
		String ret = "<";
		for (Edge edge : edges) {
			ret += edge;
			ret += ", ";
		}
		if (edges.size() > 0) {
			ret = ret.substring(0, ret.length() - 2) + ">";
		} else {
			ret += ">";
		}
		
		return ret;
	}

	public Object clone() {
		return new EgoCentricLabel(this);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		if (edges == null) {
			edges = new ArrayList<Edge>();
		} else {
			edges.clear();
		}

		for (int ii = 0; ii < size; ii++) {
			Edge edge = new Edge();
			edge.readFields(in);
			edges.add(edge);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (edges == null) {
			out.writeInt(0);
			return;
		}

		out.writeInt(edges.size());
		for (Edge edge : edges) {
			edge.write(out);
		}
	}
}

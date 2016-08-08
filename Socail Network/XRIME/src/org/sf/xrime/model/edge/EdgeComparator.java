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

import java.util.Comparator;

/**
 * Used to compare two directed edges according to their from end id and 
 * to end id.
 */
public class EdgeComparator implements Comparator<Edge> {
	@Override
	public int compare(Edge o1, Edge o2) {
		if(o1.getFrom().compareTo(o2.getFrom())==0) {
			return o1.getTo().compareTo(o2.getTo());
		}
		return o1.getFrom().compareTo(o2.getFrom());
	}
}

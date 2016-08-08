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
 * Class used to compare two edges according to the ids of the vertexes on the
 * other end.
 */
public class AdjVertexEdgeWithLabelComparator implements
		Comparator<AdjVertexEdgeWithLabel> {

	@Override
	public int compare(AdjVertexEdgeWithLabel o1, AdjVertexEdgeWithLabel o2) {
		return o1.getOpposite().compareTo(o2.getOpposite());
	}

}

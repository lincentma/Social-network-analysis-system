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
package org.sf.xrime.algorithms.partitions.connected.weakly.alg_2;

import org.sf.xrime.model.Element;
import org.sf.xrime.model.label.LabelAdder;
import org.sf.xrime.model.label.Labelable;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;

/**
 * Used to add initial labels to LabeledAdjSetVertex, when transforming AdjSetVertex to
 * LabeledAdjSetVertex. Only used in this WCC algorithm.
 * @author xue
 */
public class InitialLabelsAdder extends LabelAdder {
  public void addLabels(Labelable labels, Element element) {
    if(element instanceof LabeledAdjSetVertex){
      ((LabeledAdjSetVertex) element).setStringLabel(ConstantLabels.LAST_LABEL,
          ((LabeledAdjSetVertex) element).getId());
      // Make it an invalid vertex id.
      ((LabeledAdjSetVertex) element).setStringLabel(ConstantLabels.LABEL_BEFORE_LAST,
          ConstantLabels.INVALID_ID);
    }else{
      // Do nothing.
    }
  }
}

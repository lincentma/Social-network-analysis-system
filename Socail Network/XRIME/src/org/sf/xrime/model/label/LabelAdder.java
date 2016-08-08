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

import org.apache.hadoop.mapred.JobConf;
import org.sf.xrime.model.Element;


/**
 * Used in Vertex2LabeledTransformer, to add label to converted Labeled*Vertex.
 * @author Cai Bin
 */
public abstract class LabelAdder {
    /**
     * The user should implement this method to add your own label to corresponding Vertex.
     * @param labels Labelable of Labeled*Vertex.
     * @param element the corresponding Vertex.
     */
    public abstract void addLabels(Labelable labels, Element element);    
    
    public void configure(JobConf job)
    {
    	// do nothing
    }
    
    public void close()
    {
    	// do nothing
    }
}

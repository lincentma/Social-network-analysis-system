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
package org.sf.xrime.algorithms.pagerank.priors;

import org.sf.xrime.algorithms.pagerank.normal.PageRankAlgorithm;

/**
 * Algorithm that extends the PageRank algorithm by incorporating root nodes (priors).
 * Same as PageRankAlgorithm except data pre-processing.
 * @author Cai Bin
 * @see "Algorithms for Estimating Relative Importance in Graphs by Scott White and Padhraic Smyth, 2003"
 */
public class PageRankWithPriorsAlgorithm extends PageRankAlgorithm {
    public PageRankWithPriorsAlgorithm() {
      super();
    }  
}

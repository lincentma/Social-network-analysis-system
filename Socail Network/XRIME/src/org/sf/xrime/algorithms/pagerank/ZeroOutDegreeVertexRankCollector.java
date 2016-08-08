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
package org.sf.xrime.algorithms.pagerank;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * ZeroOutDegreeRankCollector is used by PageRankMapper to submit rank in vertex which out degree is zero.
 * The mechanism in org.apache.hadoop.ipc will be used here for communication between Mapper and JobTracker.
 * @author Cai Bin
 */
public interface ZeroOutDegreeVertexRankCollector extends VersionedProtocol {
  /**
   * To commit the score of vertex which is without out link.
   * @param taskID the mapper's taskid. Mapper may be invoked several times, but we must collect score once.
   * @param count the count of zero outDegree vertex.
   * @param rank  the total rank score collected.
   */
  public void postRank(String taskID, long count, double rank);
}

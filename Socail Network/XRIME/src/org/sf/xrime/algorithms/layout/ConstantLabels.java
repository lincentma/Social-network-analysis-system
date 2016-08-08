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
package org.sf.xrime.algorithms.layout;

/**
 * Constant labels or other strings used in the grid variant of Fruchterman-Reingold
 * algorithm.
 * @author xue
 */
public class ConstantLabels {
  /**
   * Common for all layout algorithms.
   */
  public static final String MAX_X_COORDINATE = "max_x";
  public static final String MAX_Y_COORDINATE = "max_y";
  public static final String X_COORDINATE = "x";
  public static final String Y_COORDINATE = "y";
  public static final String NUM_OF_VERTEXES = "num_of_vertexes";
  
  /**
   * Directed-Force algorithms.
   */
  public static final String ITERATIONS = "iterations";
  public static final String START_SEQ_NUM = "start_iterations";
  public static final String TEMPERATURE = "temperature";
  public static final String X_DISP = "x_disp";
  public static final String Y_DISP = "y_disp";
  
  /**
   * Others.
   */
  public static final String SURROUNDING = "surrounding";
  public static final String OPPO_ID = "oppo_id";
  public static final String SEQUENTIAL_NUM = "sq_id";
}

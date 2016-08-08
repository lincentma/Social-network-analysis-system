/*
 * Copyright (C) quna@BUPT. 2009.
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
package org.sf.xrime.algorithms.HITS;

import org.apache.hadoop.util.ToolRunner;


public class Runner {
  public static void main(String[] args) {
    runHITS(args);
  }
  
  public static void runHITS(String[] args){
    try {
      int res = ToolRunner.run(new HITSAlgorithm(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

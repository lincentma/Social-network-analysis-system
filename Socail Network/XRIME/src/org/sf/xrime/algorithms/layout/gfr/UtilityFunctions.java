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
package org.sf.xrime.algorithms.layout.gfr;

/**
 * Group utility functions used in this layout algorithm.
 * @author xue
 */
public class UtilityFunctions {
  /**
   * Calculate the area of a frame.
   * @param w
   * @param l
   * @return
   */
  public static int area(int w, int l){
    return w*l;
  }
  /**
   * Calculate the value of k as a double number. 
   * @param w
   * @param l 
   * @param num_of_vertexes
   * @return
   */
  public static double k(int w, int l, int num_of_vertexes){
    return Math.sqrt(((double)(w*l))/num_of_vertexes);
  }
  /**
   * Calculate basic attractive force.
   * @param k
   * @param d
   * @return
   */
  public static double f_a(double k, double d){
    return (d*d)/k;
  }
  /**
   * Calculate basic repulsive force.
   * @param k
   * @param d
   * @return
   */
  public static double f_r(double k, double d){
    return (k*k)/d;
  }
  /**
   * Calculate the x index of the grid in which the vertex with specified x
   * coordinate lives. NOTE: length of a side of a grid box is 2k. NOTE: the
   * index starts from zero.
   * @param x
   * @param k
   * @return
   */
  public static int grid_x_index(double x, double k){
    return (int)Math.floor(x/(2*k));
  }
  /**
   * Calculate the y index of the grid in which the vertex with specified y
   * coordinate lives.
   * @param y
   * @param k
   * @return
   */
  public static int grid_y_index(double y, double k){
    return (int)Math.floor(y/(2*k));
  }
  /**
   * Get the string representation of a grid box's indexes.
   * @param x
   * @param y
   * @return
   */
  public static String grid_index_as_string(int x, int y){
    return "( " + x + " , " + y +" )";
  }
  /**
   * Cooling down.
   * @param temperature current temperature.
   * @param k the constant k.
   * @param iterations how many iterations left.
   * @return
   */
  public static int cool(int temperature, double k, int iterations) {
    // Minimum displacement. This is a tricky point.
    int min_alter = (int)(k/2);
    // Control the cooling down speed. This is a tricky point.
    int result = temperature*4/5;
    if(result>min_alter) return result;
    else return min_alter;
  }
}

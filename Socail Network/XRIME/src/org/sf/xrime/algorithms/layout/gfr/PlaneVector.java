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
 * This class represents a plane vector.
 * @author xue
 */
public class PlaneVector {
  /**
   * The two coordinates.
   */
  private double x;
  private double y;
  /**
   * Disable default constructor.
   */
  @SuppressWarnings("unused")
  private PlaneVector(){
  }
  /**
   * Constructor.
   * @param x
   * @param y
   */
  public PlaneVector(double x, double y){
    this.x = x;
    this.y = y;
  }
  /**
   * Return the magnitude of this plane vector.
   * @return
   */
  public double magnitude(){
    return Math.sqrt(x*x+y*y);
  }
  /**
   * Return normalization of this plane vector.
   * @return
   */
  public PlaneVector normalize(){
    return new PlaneVector(x/magnitude(), y/magnitude());
  }
  /**
   * Add two plane vectors.
   * @param rhv
   * @return
   */
  public PlaneVector plus(PlaneVector rhv){
    return new PlaneVector(x+rhv.x, y+rhv.y);
  }
  /**
   * Minus a plane vector from this plane vector.
   * @param rhv
   * @return
   */
  public PlaneVector minus(PlaneVector rhv){
    return new PlaneVector(x-rhv.x, y-rhv.y);
  }
  /**
   * Multiply this plane vector with specified scalar.
   * @param s
   * @return
   */
  public PlaneVector multiply_scalar(double s){
    return new PlaneVector(x*s, y*s);
  }
  
  public double getX(){
    return x;
  }
  
  public double getY(){
    return y;
  }
}
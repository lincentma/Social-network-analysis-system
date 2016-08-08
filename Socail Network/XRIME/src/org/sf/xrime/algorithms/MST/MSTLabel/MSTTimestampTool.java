/*
 * Copyright (C) yangyin@BUPT. 2009.
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
package org.sf.xrime.algorithms.MST.MSTLabel;

/**
 * This class is used to produce time stamp used in MST messages
 * A MST time stamp is made up with two parts which are:
 *   the total milliseconds from epoch to now,
 *   a linear increasing variable.
 * @author YangYin
 *
 */
public class MSTTimestampTool {
  
  public static long tag = 0;
  
  public static int maxLen = 19;
  
  public void setTag(int newTag) {
    tag = newTag;
  }
  
  public long getTag()  {
    return tag;
  }
  
  public static String getCurTimeStamp()  {
    String curTimeStamp = Long.toString(System.currentTimeMillis());
    String strTag = Long.toString(tag);
    //the linear increasing variable should be filled to be lenght of 19
    while(strTag.length() < 19)
      strTag = "0" + strTag;
    curTimeStamp += strTag;
    tag++;
    return curTimeStamp;
  }
}

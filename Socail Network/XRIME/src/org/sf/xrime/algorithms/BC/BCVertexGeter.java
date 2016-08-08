/*
 * Copyright (C) yangcheng@BUPT. 2009.
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
package org.sf.xrime.algorithms.BC;
import java.io.*;
import java.util.*;


/*
 * @author Yang Cheng
 * The function is to get all the vertexes from the initial input
 */
public class BCVertexGeter {
  /**
   * @param args
   */
  String path;
  Set<String> BCVertex=new HashSet<String>();
  
  public BCVertexGeter(String path)
  {
    this.path=path;
  }
  public Set<String> getBCVertex()
  {  try{
    File input=new File(path);
    FileReader fr=new FileReader(input);
    BufferedReader br=new BufferedReader(fr);
    boolean end=false;
    while(!end)
    {
      String cur=br.readLine();
      String[] line=cur.split(" ");
      
      String signal=line[0];
      String vertex=line[1];
      if(signal.equals("e"))
      {
        end=true;
        continue;
      }
      BCVertex.add(vertex);
      
    }  
    fr.close();
    br.close();
  }catch(IOException e)
  {
    e.printStackTrace();
  }
    return BCVertex;
  }
    
}

package org.sf.xrime.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.sf.xrime.model.Element;

/**
 * This is a utility class used to read Element object from input stream and return it. 
 * @author xue
 */

public class ElementReader {
  /**
   * Read an Element object from the specified input stream.
   * @param type
   * @param stream
   * @return
   */
  public Element readElement(Class<?> type, InputStream stream){
    Element result = null;
    DataInput input = new DataInputStream(stream);
    try {
      result = (Element) type.newInstance();
    } catch (InstantiationException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    }
    try {
      result.readFields(input);
    } catch (IOException e) {
      return null;
    }
    return result;
  }
  
  /**
   * Shortcut method, read an Element object from specified byte array. 
   * @param type
   * @param array
   * @param offset
   * @param length
   * @return
   */
  public Element readElement(Class<?> type, byte[] array, int offset, int length){
    ByteArrayInputStream stream = new ByteArrayInputStream(array,offset,length);
    return readElement(type, stream);
  }
  
  /**
   * Another shortcut method. 
   * @param type
   * @param array
   * @return
   */
  public Element readElement(Class<?> type, byte[] array){
    ByteArrayInputStream stream = new ByteArrayInputStream(array);
    return readElement(type, stream);
  }
}

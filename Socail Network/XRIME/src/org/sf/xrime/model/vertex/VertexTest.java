package org.sf.xrime.model.vertex;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.sf.xrime.model.vertex.Vertex;


public class VertexTest {
	@Test
	public void testVertexReadWrite() throws IOException {
		ByteArrayOutputStream  strOutputStream=new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(strOutputStream);
			
		Vertex vertex1=new Vertex("vertex1");
		Vertex vertex2=new Vertex("vertex2");
		
		vertex1.write(out);
		vertex2.write(out);

		ByteArrayInputStream  strInputStream=new ByteArrayInputStream(strOutputStream.toByteArray());
		DataInputStream in = new DataInputStream(strInputStream);
		
		Vertex vertex=new Vertex();
		vertex.readFields(in);
		
		assertTrue(vertex1.getId().compareTo(vertex.getId())==0);
		
		vertex.readFields(in);
		assertTrue(vertex2.getId().compareTo(vertex.getId())==0);	
	}
}

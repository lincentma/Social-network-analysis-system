package org.sf.xrime.model.edge;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.sf.xrime.model.edge.Edge;


public class EdgeTest {
	@Test
	public void testEdgeReadWrite() throws IOException {
		ByteArrayOutputStream  strOutputStream=new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(strOutputStream);
			
		Edge edge1=new Edge("from1","to1");
		Edge edge2=new Edge("from2","to2");
		Edge edge3=new Edge("from2",null);
		
		edge1.write(out);
		edge2.write(out);
		edge3.write(out);

		ByteArrayInputStream  strInputStream=new ByteArrayInputStream(strOutputStream.toByteArray());
		DataInputStream in = new DataInputStream(strInputStream);
		
		Edge edge=new Edge();
		edge.readFields(in);
		
		assertEquals(edge.getFrom(),edge1.getFrom());
		assertEquals(edge.getTo(),edge1.getTo());
		
		edge.readFields(in);
		assertEquals(edge.getFrom(),edge2.getFrom());
		assertEquals(edge.getTo(),edge2.getTo());
		
		edge.readFields(in);
		assertEquals(edge.getFrom(),edge3.getFrom());
		assertEquals(edge.getTo(),null);
	}
}

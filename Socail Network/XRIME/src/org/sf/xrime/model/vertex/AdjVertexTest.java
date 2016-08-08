package org.sf.xrime.model.vertex;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.junit.Test;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjVertex;


public class AdjVertexTest {
	@Test
	public void testAdjVertexReadWrite() throws IOException {
		ByteArrayOutputStream  strOutputStream=new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(strOutputStream);
		
		AdjVertex adjVertex1=new AdjVertex("from1");
		Edge edge1=new Edge("from1","to1");
		Edge edge2=new Edge("from1","to2");

		adjVertex1.addEdge(edge1);
		adjVertex1.addEdge(edge2);
				
		adjVertex1.write(out);

		ByteArrayInputStream  strInputStream=new ByteArrayInputStream(strOutputStream.toByteArray());
		DataInputStream in = new DataInputStream(strInputStream);
		
		AdjVertex adjVertex=new AdjVertex();
		adjVertex.readFields(in);
		
		assertEquals(adjVertex.getId(),adjVertex1.getId());
		assertEquals(adjVertex.getEdges().size(),adjVertex1.getEdges().size());
		assertEquals(adjVertex.getEdges().get(0).getFrom(),adjVertex1.getEdges().get(0).getFrom());
		assertEquals(adjVertex.getEdges().get(0).getTo(),adjVertex1.getEdges().get(0).getTo());
		assertEquals(adjVertex.getEdges().get(1).getFrom(),adjVertex1.getEdges().get(1).getFrom());
		assertEquals(adjVertex.getEdges().get(1).getTo(),adjVertex1.getEdges().get(1).getTo());
	}
	
	@Test
	public void testAdjVertexReadWriteNull() throws IOException {
		ByteArrayOutputStream  strOutputStream=new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(strOutputStream);
		
		AdjVertex adjVertex1=new AdjVertex("from1");		
		adjVertex1.write(out);

		ByteArrayInputStream  strInputStream=new ByteArrayInputStream(strOutputStream.toByteArray());
		DataInputStream in = new DataInputStream(strInputStream);
		
		AdjVertex adjVertex=new AdjVertex();
		adjVertex.readFields(in);
		
		assertEquals(adjVertex.getId(),adjVertex1.getId());
		assertEquals(adjVertex.getEdges().size(), 0);
		assertEquals(adjVertex1.getEdges().size(), 0);
	}
	
	@Test
	public void testAdjVertexReadWriteEdge() throws IOException {
		ByteArrayOutputStream  strOutputStream=new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(strOutputStream);
		
		AdjVertex adjVertex1=new AdjVertex("from1");
		Edge4Test edge1=new Edge4Test("from1", "to1", "info1");
		Edge4Test edge2=new Edge4Test("from1", "to2", "info2");

		adjVertex1.addEdge(edge1);
		adjVertex1.addEdge(edge2);
		
		adjVertex1.write(out);

		ByteArrayInputStream  strInputStream=new ByteArrayInputStream(strOutputStream.toByteArray());
		DataInputStream in = new DataInputStream(strInputStream);
		
		AdjVertex adjVertex=new AdjVertex();
		adjVertex.readFields(in);
		
		assertEquals(adjVertex.getId(),adjVertex1.getId());
		assertEquals(adjVertex.getEdges().size(),adjVertex1.getEdges().size());
		
		Edge edgeVertex;
		Edge edgeVertex1;
		Edge4Test Edge4TestVertex;
		Edge4Test Edge4TestVertex1;
		
		edgeVertex =adjVertex.getEdges().get(0);
		edgeVertex1=adjVertex.getEdges().get(0);
		assertTrue(edgeVertex  instanceof Edge4Test);
		assertTrue(edgeVertex1 instanceof Edge4Test);
		Edge4TestVertex =(Edge4Test) edgeVertex;
		Edge4TestVertex1=(Edge4Test) edgeVertex1;
		assertEquals(Edge4TestVertex.getFrom(),    Edge4TestVertex1.getFrom());
		assertEquals(Edge4TestVertex.getTo(),      Edge4TestVertex1.getTo());
		assertEquals(Edge4TestVertex.getAddInfo(), Edge4TestVertex1.getAddInfo());

		edgeVertex =adjVertex.getEdges().get(1);
		edgeVertex1=adjVertex.getEdges().get(1);
		assertTrue(edgeVertex  instanceof Edge4Test);
		assertTrue(edgeVertex1 instanceof Edge4Test);
		Edge4TestVertex =(Edge4Test) edgeVertex;
		Edge4TestVertex1=(Edge4Test) edgeVertex1;
		assertEquals(Edge4TestVertex.getFrom(),    Edge4TestVertex1.getFrom());
		assertEquals(Edge4TestVertex.getTo(),      Edge4TestVertex1.getTo());
		assertEquals(Edge4TestVertex.getAddInfo(), Edge4TestVertex1.getAddInfo());
	}
	
	@Test
	public void testAdjVertexCopyConstructor() throws IOException {
		AdjVertex adjVertex1=new AdjVertex("from1");
		Edge edge1=new Edge("from1","to1");
		Edge edge2=new Edge("from1","to2");
		Edge edge3=new Edge("from1","to3");

		adjVertex1.addEdge(edge1);
		adjVertex1.addEdge(edge2);
		adjVertex1.addEdge(edge3);
		
		AdjVertex adjVertex2=new AdjVertex(adjVertex1);
		
		assertEquals(adjVertex1.getId(), adjVertex2.getId());
		assertEquals(adjVertex1.getEdges().get(0).getTo(), adjVertex2.getEdges().get(0).getTo());
		assertEquals(adjVertex1.getEdges().get(1).getTo(), adjVertex2.getEdges().get(1).getTo());
		assertEquals(adjVertex1.getEdges().get(2).getTo(), adjVertex2.getEdges().get(2).getTo());
		assertNotSame(adjVertex1.getEdges().get(0),adjVertex2.getEdges().get(0));
		assertNotSame(adjVertex1.getEdges().get(1),adjVertex2.getEdges().get(1));
		assertNotSame(adjVertex1.getEdges().get(2),adjVertex2.getEdges().get(2));
	}
	
	@Test
	public void testAdjVertexRemoveLoop() throws IOException {
		AdjVertex adjVertex1=new AdjVertex("from1");
		Edge edge1=new Edge("from1","to1");
		Edge edge2=new Edge("from1","from1");
		Edge edge3=new Edge("from1","from1");

		adjVertex1.addEdge(edge1);
		adjVertex1.addEdge(edge2);
		adjVertex1.addEdge(edge1);
		adjVertex1.addEdge(edge3);
		
		assertTrue(adjVertex1.getEdges().size()==4);
		adjVertex1.removeLoop();
		assertTrue(adjVertex1.getEdges().size()==2);
	}
	
	@Test
	public void testAdjVertexRemoveMultipleEdge() throws IOException {
		AdjVertex adjVertex1=new AdjVertex("from1");
		Edge edge1=new Edge("from1","to1");
		Edge edge2=new Edge("from1","from1");
		Edge edge3=new Edge("from1","from1");

		adjVertex1.addEdge(edge1);
		adjVertex1.addEdge(edge2);
		adjVertex1.addEdge(edge1);
		adjVertex1.addEdge(edge3);
		
		assertTrue(adjVertex1.getEdges().size()==4);
		adjVertex1.removeMultipleEdge();
		assertTrue(adjVertex1.getEdges().size()==2);
	}
	
	static class Edge4Test extends Edge implements Writable, Cloneable {
		private String addInfo;
		
		static {
			WritableFactories.setFactory
			      (Edge4Test.class,
			       new WritableFactory() {
			           public Writable newInstance() { return new Edge4Test(); }
			       });
	    }
		
		public Edge4Test() {			
		}

		public Edge4Test(String from, String to, String addInfo) {
			super(from,to);
			this.addInfo=addInfo;
		}
		
		public String getAddInfo() {
			return addInfo;
		}

		public void setAddInfo(String addInfo) {
			this.addInfo = addInfo;
		}
		
		public Object clone() {
			return new Edge4Test(this.from, this.to, this.addInfo);
		}

		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			this.addInfo = Text.readString(in);
		}

		public void write(DataOutput out) throws IOException {
			super.write(out);
			Text.writeString(out, addInfo);
		}
	}
}

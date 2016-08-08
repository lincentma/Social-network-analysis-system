package org.sf.xrime.model.label;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import org.sf.xrime.model.label.Labels;

public class LabelsTest {
	@Test
	public void testOperation() throws Exception {
		Labels label1=new Labels();
		Labels label2=new Labels(label1);
		
		Map<String, Writable> map1=label1.getLabels();
		Map<String, Writable> map2=label2.getLabels();
		
		assertTrue(map1!=map2);
		
		Text test=new Text("test1");
		label1.setLabel("name1", test);
		Text output=(Text) label1.getLabel("name1");
		assertTrue(test==output);
		
		Labels label3=new Labels(label1);
		output=(Text) label3.getLabel("name1");
		assertTrue(test==output);
		
		label1.setIntLabel("name2", 231);
		assertTrue(label1.getIntLabel("name2")==231);
		assertTrue(label1.getIntLabel("name3",112)==112);
		label1.setIntLabel("name2", 111);
		
		assertEquals(label1.toString(),"<name1, test1>, <name2, 111>");
		
		label1.removeLabel("name2");
		assertTrue(label1.getIntLabel("name2",112)==112);		
		assertEquals(label1.toString(),"<name1, test1>");
	}
	
	@Test
	public void testEdgeReadWrite() throws IOException, Exception {
		ByteArrayOutputStream  strOutputStream=new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(strOutputStream);
		
		Labels label1=new Labels();
		Text test=new Text("test1");
		label1.setLabel("name1", test);
		label1.setIntLabel("name2", 231);
		label1.setIntLabel("name3", 111);
		
		label1.write(out);
		
		ByteArrayInputStream  strInputStream=new ByteArrayInputStream(strOutputStream.toByteArray());
		DataInputStream in = new DataInputStream(strInputStream);
		
		Labels label2=new Labels();
		label2.readFields(in);
		
		Map<String, Writable> map2=label2.getLabels();
		assertTrue(map2.size()==3);
		assertTrue(label2.getIntLabel("name2")==231);
		assertTrue(label2.getIntLabel("name3")==111);
		Text output=(Text) label2.getLabel("name1");
		assertTrue(test.toString().compareTo(output.toString())==0);	
	}
}

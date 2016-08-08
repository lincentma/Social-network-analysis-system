package org.sf.xrime;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.sf.xrime.model.edge.EdgeTest;
import org.sf.xrime.model.label.LabelsTest;
import org.sf.xrime.model.vertex.AdjVertexTest;
import org.sf.xrime.model.vertex.VertexTest;
import org.sf.xrime.utils.SequenceTempDirMgrTest;


@RunWith(Suite.class)@Suite.SuiteClasses({  
	EdgeTest.class,  
	LabelsTest.class,  
	AdjVertexTest.class,    
	VertexTest.class,  
	SequenceTempDirMgrTest.class})
	
public class AllTests {
	// why on earth I need this class, I have no idea! }
}

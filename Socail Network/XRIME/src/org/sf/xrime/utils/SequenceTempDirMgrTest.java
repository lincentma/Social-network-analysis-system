package org.sf.xrime.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;
import org.sf.xrime.utils.SequenceTempDirMgr;

public class SequenceTempDirMgrTest {
	@Test
	public void testGetTempDir() throws IOException {
		SequenceTempDirMgr mgr=new SequenceTempDirMgr("test", 2);

		mgr.getTempDir();
		Path path2=mgr.getTempDir();
		mgr.getTempDir();
		mgr.getTempDir();
		
		assertTrue(mgr.getDirs().size()==4);
		assertTrue(mgr.getSeqNum()==6);
		mgr.removePath(path2);
		assertTrue(mgr.getDirs().size()==3);
		mgr.deleteAll();
		assertTrue(mgr.getDirs().size()==3);
		
		FileSystem4Test testFileSystem=new FileSystem4Test();

		mgr.setFileSystem(testFileSystem);
		mgr.deleteAll();
		assertTrue(mgr.getDirs().size()==0);
		assertTrue(testFileSystem.getDirs().size()==3);
		
		testFileSystem.addExisted(new Path(mgr.getPrefix()+Integer.toString(mgr.getSeqNum())));
		path2=mgr.getTempDir();
		assertTrue(mgr.getSeqNum()==8);
	}
	
	static class FileSystem4Test extends FileSystem {
		protected Set<Path> dirs;
		protected Set<Path> existed;

		public FileSystem4Test() {
			dirs=new HashSet<Path>();
			existed=new HashSet<Path>();
		}
		
		public void addExisted(Path path) {
			existed.add(path);
		}
		
		public Set<Path> getDirs() {
			return dirs;
		}

		public void setDirs(Set<Path> dirs) {
			this.dirs = dirs;
		}		
		
		public boolean exists(Path f) throws IOException {
			return existed.contains(f);
		}
		
		@Override
		public FSDataOutputStream append(Path f, int bufferSize,
				Progressable progress) throws IOException {
			return null;
		}

		@Override
		public FSDataOutputStream create(Path f, FsPermission permission,
				boolean overwrite, int bufferSize, short replication,
				long blockSize, Progressable progress) throws IOException {
			return null;
		}

		@Override
		public boolean delete(Path f) throws IOException {
			dirs.add(f);
			return false;
		}

		@Override
		public boolean delete(Path f, boolean recursive) throws IOException {
			dirs.add(f);
			return false;
		}

		@Override
		public FileStatus getFileStatus(Path f) throws IOException {
			return null;
		}

		@Override
		public URI getUri() {
			return null;
		}

		@Override
		public Path getWorkingDirectory() {
			return null;
		}

		@Override
		public void initialize(URI name, Configuration conf) throws IOException {
			
		}

		@Override
		public FileStatus[] listStatus(Path f) throws IOException {
			return null;
		}

		@Override
		public boolean mkdirs(Path f, FsPermission permission)
				throws IOException {
			return false;
		}

		@Override
		public FSDataInputStream open(Path f, int bufferSize)
				throws IOException {
			return null;
		}

		@Override
		public boolean rename(Path src, Path dst) throws IOException {
			return false;
		}

		@Override
		public void setWorkingDirectory(Path new_dir) {			
		}
	}
}

package com.rapidminer.extension.utilities;


import java.io.File;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;




public class Helper {
	public static Object[][] convert2D(ArrayList<Object[]> arrayList) {
		Object[][] array2D = new Object[arrayList.size()][];
		for (int i = 0; i < array2D.length; i++) {
			Object[] row = arrayList.get(i);
			array2D[i] = row;
		}
		return array2D;
	}

	public static String[][] convert2DString(ArrayList<String[]> arrayList) {
		String[][] array2D = new String[arrayList.size()][];
		for (int i = 0; i < array2D.length; i++) {
			String[] row = arrayList.get(i);
			array2D[i] = row;
		}
		return array2D;
	}
	// Xóa file trong Hadoop Cluster
	public static void deleteFolderHadoopCluster(Configuration conf, String folderPath) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
	
	// Xóa file trong Hadoop Local
	public static void deleteFolderHadoopLocal(String folderPath) throws Exception {
		File file=new File(folderPath);
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
	}
}

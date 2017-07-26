package com.rapidminer.pagerank.hadoop.utilities;

import java.util.ArrayList;
import java.util.concurrent.FutureTask;


import com.rapidminer.example.ExampleSet;
/**
 * 
 * @author Khanh Duy Pham
 *
 */
public class MaxtriHelper {
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

	public static Object[][] convert2DObject(ArrayList<ArrayList<Object>> arrayList) {
		Object[][] array2D = new Object[arrayList.size()][];
		for (int i = 0; i < array2D.length; i++) {
			ArrayList<Object> row = arrayList.get(i);
			array2D[i] = row.toArray(new Object[row.size()]);
		}
		return array2D;
	}
	public static String[][] convert2DString1(ArrayList<ArrayList<String>> arrayList) {
		String[][] array2D = new String[arrayList.size()][];
		for (int i = 0; i < array2D.length; i++) {
			ArrayList<String> row = arrayList.get(i);
			array2D[i] = row.toArray(new String[row.size()]);
		}
		return array2D;
	}



	// Lấy dữ liệu từ Hadoop Cluster
	public static ExampleSet getDataHadoopCluster(String pathName) throws Exception {
		FutureTask<ExampleSet> futureTask = new FutureTask<>(new ReadFileHadoopLocalCallable(pathName));
		futureTask.run();
		return futureTask.get();
	}
}

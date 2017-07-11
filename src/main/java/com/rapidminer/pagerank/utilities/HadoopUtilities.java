package com.rapidminer.pagerank.utilities;

import java.io.File;
import java.util.concurrent.FutureTask;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rapidminer.example.ExampleSet;

public class HadoopUtilities {

	/**
	 * Get data from Hadoop local
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static ExampleSet getDataHadoopLocal(String path) throws Exception {
		FutureTask<ExampleSet> futureTask = new FutureTask<>(new ReadFileHadoopLocalCallable(path));
		futureTask.run();
		return futureTask.get();
	}

	/**
	 * Get data from Hadoop cluster
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static ExampleSet getDataHadoopCluster(String path) throws Exception {
		FutureTask<ExampleSet> futureTask = new FutureTask<>(new ReadFileHadoopClusterCallable(path));
		futureTask.run();
		return futureTask.get();
	}

	/**
	 * Write data from local to hadoop cluster
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static boolean writeHadoopCluster(String path, Configuration conf) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path pathLocal = new Path(path);
		Path pathHadoop = new Path(HadoopCluster.DEFAULT_FS_INPUT);
		if (fs.exists(pathLocal)) {
			fs.delete(pathHadoop, true);
			fs.copyFromLocalFile(pathLocal, pathHadoop);
			return true;
		}
		return false;
	}

	/**
	 * Delete folderPath from Hadoop cluster
	 * 
	 * @param conf
	 * @param folderPath
	 * @throws Exception
	 */
	public static void deleteFolderHadoopCluster(Configuration conf, String folderPath) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	/**
	 * Delete file form local
	 * 
	 * @param folderPath
	 * @throws Exception
	 */
	public static void deleteFolderHadoopLocal(String folderPath) throws Exception {
		File file = new File(folderPath);
		if (file.exists()) {
			file.delete();
		}
	}

	/**
	 * Delete folderPath from Hadoop local
	 * 
	 * @param folderPath
	 * @throws Exception
	 */
	public static void deleteFolderHadoop(String folderPath) throws Exception {
		File file = new File(folderPath);
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
	}
}

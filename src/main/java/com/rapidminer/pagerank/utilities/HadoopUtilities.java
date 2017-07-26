package com.rapidminer.pagerank.utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.tools.FileSystemService;

/**
 * 
 * @author Khanh Duy Pham
 *
 */
public class HadoopUtilities {
	public static final String PATH_RAPIDMINER = FileSystemService.getUserRapidMinerDir().toString();

	/**
	 * Get data from Hadoop local
	 * 
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
	 * 
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
	public static boolean writeHadoopCluster(String src, String desc) throws Exception {
		FileSystem fs = FileSystem.get(HadoopCluster.getConf());
		Path pathLocal = new Path(src);
		Path pathHadoop = new Path(desc);
		if (fs.exists(pathLocal)) {
			fs.delete(pathHadoop, true);
			fs.copyFromLocalFile(pathLocal, pathHadoop);
			return true;
		}
		return false;
	}
	
	/**
	 * Write data from local to hadoop cluster
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static boolean copyFromLocalFileToHadoop(String src, String desc) throws Exception {
		FileSystem fs = FileSystem.get(HadoopCluster.getConf());
		Path pathLocal = new Path(src);
		Path pathHadoop = new Path(desc);
		if (fs.exists(pathHadoop)) {
			fs.delete(pathHadoop, true);
			fs.copyFromLocalFile(true, true, pathLocal, pathHadoop);
			
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

	public static boolean checkFileExistHadoopCluster(String pathName) throws IOException {
		FileSystem fs = FileSystem.get(HadoopCluster.getConf());
		Path path = new Path(pathName);
		return fs.exists(path);
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

	/**
	 * Save ExampleSet to file
	 * 
	 * @param exampleSet
	 */
	public static boolean saveExampleSetToFile(ExampleSet exampleSet) {
		String input = HadoopUtilities.PATH_RAPIDMINER + "/extensions/workspace/input.txt";
		Attributes attributes = exampleSet.getAttributes();
		if (attributes.size() > 2) {
			try {
				/*
				 * File file=new File(HadoopUtilities.PATH_RAPIDMINER +
				 * "/pageRank/input"); if (!file.exists()) {
				 * LogService.getRoot().log(Level.CONFIG,
				 * "com.rapidminer.tools.FileSystemService.creating_directory",
				 * file); boolean result = file.mkdirs(); if (!result) {
				 * LogService.getRoot().log(Level.WARNING,
				 * "com.rapidminer.tools.FileSystemService.creating_home_directory_error",
				 * file); } }
				 */
				FileOutputStream fos = new FileOutputStream(input);
				OutputStreamWriter osw = new OutputStreamWriter(fos);
				BufferedWriter bw = new BufferedWriter(osw);
				for (Example item : exampleSet) {
					String id = item.get("id").toString();
					// String title = item.get("title").toString();
					String outlink = item.get("outlink").toString();

					if (outlink.equals("?")) {
						outlink = "";
					}

					String line = id + "\t" + 1.0 + "\t" + outlink;
					bw.write(line);
					bw.newLine();
				}
				bw.flush();
				bw.close();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
			}
		}
		return false;
	}
	
	public static Boolean runHadoop() throws Exception{
		Process p = Runtime.getRuntime().exec("L:/hadoop/bin/hadoop.cmd jar D:/PageRankLocal.jar");
		BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
		String line = input.readLine();
		while (line != null) {
		  // process output of the task
		  // ...
		}
		input.close();
		// wait for the task complete
		p.waitFor();

		return null;

	}

	public static Boolean runHadoopLocal() throws InterruptedException, ExecutionException {
		FutureTask<Boolean> futureTask = new FutureTask<>(new RunHadoopLocalCallable());
		futureTask.run();
		return futureTask.get();
	}

	public static Boolean runHadoopCluster() throws InterruptedException, ExecutionException {
		FutureTask<Boolean> futureTask = new FutureTask<>(new RunHadoopClusterCallable());
		futureTask.run();
		return futureTask.get();
	}
}

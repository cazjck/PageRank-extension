package com.rapidminer.pagerank.utilities;

import java.io.File;
import java.util.concurrent.FutureTask;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.rapidminer.example.ExampleSet;

public class HadoopCluster {
	private static Configuration conf = null;
	public static String HOST = "namenode";
	public static String PORT_YARN = "8032";
	public static String PORT_HDFS = "9000";
	public static String YARN_RESOURCE = HOST + ":" + PORT_YARN;
	public static String DEFAULT_FS = "hdfs://" + HOST + ":" + PORT_HDFS;

	public static Configuration getConf() {
		if (conf == null) {
			conf = new YarnConfiguration();
			conf.set("fs.defaultFS", DEFAULT_FS);
			conf.set("mapreduce.framework.name", "yarn");
			conf.set("yarn.resourcemanager.address", YARN_RESOURCE);
			conf.set("yarn.resourcemanager.hostname", HOST);
			// conf.set("os.name", "Ubuntu");
			conf.set(MRConfig.MAPREDUCE_APP_SUBMISSION_CROSS_PLATFORM, "true");

		}
		return conf;
	}

	// Lấy dữ liệu từ localhost
	public static ExampleSet getDataHadoopLocal(String pathName) throws Exception {
		FutureTask<ExampleSet> futureTask = new FutureTask<>(
				new ReadFileHadoopLocalCallable(pathName));
		futureTask.run();
		return futureTask.get();
	}

	// Lấy dữ liệu từ Hadoop Cluster
	public static ExampleSet getDataHadoopCluster(String pathName) throws Exception {
		FutureTask<ExampleSet> futureTask = new FutureTask<>(
				new ReadFileHadoopClusterCallable(pathName));
		futureTask.run();
		return futureTask.get();
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
		File file = new File(folderPath);
		if (file.exists()) {
			file.delete();
		}
	}

	// Xóa file trong Hadoop Local
	public static void deleteFolderHadoop(String folderPath) throws Exception {
		File file = new File(folderPath);
		if (file.exists()) {
			FileUtils.deleteDirectory(file);
		}
	}

}

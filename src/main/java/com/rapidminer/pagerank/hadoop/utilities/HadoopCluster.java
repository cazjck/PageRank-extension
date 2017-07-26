package com.rapidminer.pagerank.hadoop.utilities;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class HadoopCluster {
	private static Configuration conf = null;
	public static String HOST = "namenode";
	public static String PORT_YARN = "8032";
	public static String PORT_HDFS = "9000";
	public static final String YARN_RESOURCE = HOST + ":" + PORT_YARN;
	public static final String DEFAULT_FS = "hdfs://" + HOST + ":" + PORT_HDFS;
	public static final String DEFAULT_FS_INPUT = DEFAULT_FS + "/input";
/**
 * Instance Hadoop Cluster
 * @return
 */
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

}

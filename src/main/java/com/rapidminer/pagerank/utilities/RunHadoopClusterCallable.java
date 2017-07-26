package com.rapidminer.pagerank.utilities;

import java.util.concurrent.Callable;


import com.rapidminer.pagerank.hadoop.PageRankDriver;

/**
 * 
 * @author Khanh Duy Kham
 *
 */
public class RunHadoopClusterCallable implements Callable<Boolean> {


	@Override
	public Boolean call() throws Exception {
		return PageRankDriver.runPageRankHadoopCluster();
	}


}

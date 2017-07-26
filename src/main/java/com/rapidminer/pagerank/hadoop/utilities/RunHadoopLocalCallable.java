package com.rapidminer.pagerank.hadoop.utilities;


import java.util.concurrent.Callable;

import com.rapidminer.pagerank.hadoop.PageRankDriver;


public class RunHadoopLocalCallable implements Callable<Boolean> {


	@Override
	public Boolean call() throws Exception {
		return PageRankDriver.runPageRankHadoopLocal();
	}

}

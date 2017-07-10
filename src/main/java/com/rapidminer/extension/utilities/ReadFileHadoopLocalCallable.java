package com.rapidminer.extension.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;



public class ReadFileHadoopLocalCallable implements Callable<ExampleSet> {
	FileInputStream fis;
	InputStreamReader is;
	BufferedReader br;
	//ArrayList<String[]> arrayList;
	ArrayList<ArrayList<String>> arrayList;
	String line;

	public ReadFileHadoopLocalCallable(String path) throws Exception {
		fis = new FileInputStream(new File(path + "/part-r-00000"));
		is = new InputStreamReader(fis);
		br = new BufferedReader(is);
		arrayList = new ArrayList<>();

	}

/*	@Override
	public ExampleSet call() throws Exception {
		while ((line = br.readLine()) != null) {
			String[] s = line.split("\t");	
			arrayList.add(s);
		}	
		String[][] array2D = new String[arrayList.size()][];
		for (int i = 0; i < array2D.length; i++) {
		    String[] row = arrayList.get(i);
		    array2D[i] = row;
		}
		System.out.println(array2D.toString());
		return ExampleSetFactory.createExampleSet(array2D);
	}*/
	
	@Override
	public ExampleSet call() throws Exception {
		while ((line = br.readLine()) != null) {
			String[] s = line.split("\t");	
			ArrayList<String> arr=new ArrayList<>();
			for (int i = 0; i < s.length; i++) {
				arr.add(s[i]);
			}
			arrayList.add(arr);
		}	
		//System.out.println(arrayList.toString());
		String[][] array2D = new String[arrayList.size()][];
		for (int i = 0; i < array2D.length; i++) {
		    ArrayList<String> row = arrayList.get(i);
		    array2D[i] = row.toArray(new String[row.size()]);
		}
		//System.out.println("------------------");
		//System.out.println(array2D);
		return ExampleSetFactory.createExampleSet(array2D);
	}

}

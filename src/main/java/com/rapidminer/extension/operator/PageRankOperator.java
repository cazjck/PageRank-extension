package com.rapidminer.extension.operator;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowReader;
import com.rapidminer.extension.pagerank.PageRankDriver;
import com.rapidminer.extension.utilities.ReadFileHadoopLocalCallable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.io.CSVExampleSource;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;
import com.rapidminer.studio.io.data.internal.file.csv.CSVDataSourceFactory;
import com.rapidminer.tools.LogService;

/**
 * 
 * @author Khanh Duy Pham
 *
 */
public class PageRankOperator extends Operator {
	private InputPort input = getInputPorts().createPort("input");
	private OutputPort output = getOutputPorts().createPort("result");
	public static final String PARAMETER_ADVANCED = "Advanced";
	public static final String PARAMETER_DAMPING = "Damping factor";
	public static final String PARAMETER_INTERATION = "Iteration factor";

	public PageRankOperator(OperatorDescription description) {
		super(description);
		MetaData desiredMetaData = new MetaData(ExampleSet.class);
		SimplePrecondition simplePrecondition = new SimplePrecondition(input, desiredMetaData);
		input.addPrecondition(simplePrecondition);
		getTransformer().addPassThroughRule(input, output);
	}

	@Override
	public void doWork() throws OperatorException {
		Logger logger = LogService.getRoot();
		ExampleSet exampleSet = input.getData(ExampleSet.class);

		Attributes attributes = exampleSet.getAttributes();
		if (attributes.size() > 2) {
			try {
				FileOutputStream fos = new FileOutputStream("D:/input.txt");
				OutputStreamWriter osw = new OutputStreamWriter(fos);
				BufferedWriter bw = new BufferedWriter(osw);
				for (Example item : exampleSet) {
					String id = item.get("id").toString();
					String title = item.get("title").toString();
					String outlink = item.get("outlink").toString();
/*					if (outlink == "?") {
						outlink = " ";
					}*/
					String line = id + "\t" + title + "\t" + outlink;
					bw.write(line);
					bw.newLine();
				}
				bw.flush();
				osw.flush();
				fos.flush();
				bw.close();
				osw.close();
				fos.close();
				PageRankDriver.input = "D:/input.txt";
				// if (PageRankDriver.runHadoopLocal()) {

				// }
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
			}

		}

		// logger.log(Level.INFO, text);
		/*
		 * double[][] data1 = { { 1, 2, 3, 4 }, { 5, 6, 7, 8 } }; double[][]
		 * data2 = { { 1, 2, 3, 5 }, { 5, 6, 7, 9 } }; ExampleSet s1 =
		 * ExampleSetFactory.createExampleSet(data1); ExampleSet s2 =
		 * ExampleSetFactory.createExampleSet(data2); output.deliver(s1);
		 */
		ExampleSet exampleSet2 = null;
		try {
			// exampleSet2 = PageRankDriver.getDataHadoopCluster("D:/test");
			exampleSet2 = PageRankDriver.getDataHadoopCluster(
					"J:/HK1 year 4/Do an chuyen nganh/Source Code/Hadoop_PageRank_DBLP/output/dblp2");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		output.deliver(exampleSet2);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();

		/*types.add(new ParameterTypeBoolean(PARAMETER_ADVANCED,
				"If checked, You can set property for damping, interation. Default: Damping factory is 0.85 and Interation factory 2",
				false, false));*/

		ParameterType damping = new ParameterTypeDouble(PARAMETER_DAMPING,
				"This parameter defines damping factory.",
				0.0,
				1.0,
				0.85);
		ParameterType interations = new ParameterTypeInt(PARAMETER_INTERATION,
				"This parameter defines iteration factory.",
				1,
				100,
				2);
		//type.registerDependencyCondition(new BooleanParameterCondition(this, PARAMETER_USE_CUSTOM_TEXT, true, true));

		types.add(damping);
		types.add(interations);

		return types;
	}

}

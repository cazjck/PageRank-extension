package com.rapidminer.pagerank.operator;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.util.LinkedList;
import java.util.List;

import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.nio.CSVExampleSourceConfigurationWizardCreator;
import com.rapidminer.operator.nio.model.AbstractDataResultSetReader;
import com.rapidminer.operator.nio.model.DataResultSetFactory;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.MetaData;
import com.rapidminer.operator.ports.metadata.SimplePrecondition;
import com.rapidminer.pagerank.pagerank.PageRankDriver;
import com.rapidminer.pagerank.utilities.MaxtriHelper;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeBoolean;
import com.rapidminer.parameter.ParameterTypeChar;
import com.rapidminer.parameter.ParameterTypeConfiguration;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;
import com.rapidminer.tools.DateParser;
import com.rapidminer.tools.LineParser;
import com.rapidminer.tools.StrictDecimalFormat;

public class PageRankInputOperator extends AbstractDataResultSetReader {

	private InputPort input = getInputPorts().createPort("input");
	private OutputPort output = getOutputPorts().createPort("result");
	public static final String PARAMETER_ADVANCED = "Advanced";
	public static final String PARAMETER_DAMPING = "Damping factor";
	public static final String PARAMETER_INTERATION = "Iteration factor";
	public static final String PARAMETER_TEXT_FILE = "text_file";
	public static final String PARAMETER_TRIM_LINES = "trim_lines";
	public static final String PARAMETER_SKIP_COMMENTS = "skip_comments";
	public static final String PARAMETER_COMMENT_CHARS = "comment_characters";
	public static final String PARAMETER_USE_QUOTES = "use_quotes";
	public static final String PARAMETER_QUOTES_CHARACTER = "quotes_character";
	public static final String PARAMETER_COLUMN_SEPARATORS = "column_separators";
	public static final String PARAMETER_ESCAPE_CHARACTER = "escape_character";

	public PageRankInputOperator(OperatorDescription description) {
		super(description);
		MetaData desiredMetaData = new MetaData(ExampleSet.class);
		SimplePrecondition simplePrecondition = new SimplePrecondition(input, desiredMetaData);
		input.addPrecondition(simplePrecondition);
		getTransformer().addPassThroughRule(input, output);

	}

	@Override
	public void doWork() throws OperatorException {
		// Logger logger = LogService.getRoot();
		// get value parameter
		Double damping = getParameterAsDouble(PARAMETER_DAMPING);
		int interaions = getParameterAsInt(PARAMETER_INTERATION);
		PageRankDriver.DAMPING = damping;
		PageRankDriver.INTERATIONS = interaions;

		ExampleSet exampleSet = input.getData(ExampleSet.class);
		// save file - run on hadoop
		saveFile(exampleSet);

		ExampleSet exampleSetResult = null;
		try {
			exampleSetResult = MaxtriHelper.getDataHadoopCluster(PageRankDriver.RESULT_LOCAL);
			exampleSetResult.getAttributes().get("att1").setName("rank");
			exampleSetResult.getAttributes().get("att2").setName("title");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

		output.deliver(exampleSetResult);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		LinkedList<ParameterType> types = new LinkedList<>();

		ParameterType type = new ParameterTypeConfiguration(CSVExampleSourceConfigurationWizardCreator.class, this);
		type.setExpert(false);
		types.add(type);
		types.add(makeFileParameterType());

		// Separator
		types.add(new ParameterTypeString(PARAMETER_COLUMN_SEPARATORS,
				"Column separators for data files (regular expression)", ";", false));
		types.add(new ParameterTypeBoolean(
				PARAMETER_TRIM_LINES,
				"Indicates if lines should be trimmed (empty spaces are removed at the beginning and the end) before the column split is performed. This option might be problematic if TABs are used as a seperator.",
				false));
		// Quotes
		types.add(new ParameterTypeBoolean(PARAMETER_USE_QUOTES, "Indicates if quotes should be regarded.", true, false));
		type = new ParameterTypeChar(PARAMETER_QUOTES_CHARACTER, "The quotes character.",
				LineParser.DEFAULT_QUOTE_CHARACTER, false);
		type.registerDependencyCondition(new BooleanParameterCondition(this, PARAMETER_USE_QUOTES, false, true));
		types.add(type);
		type = new ParameterTypeChar(PARAMETER_ESCAPE_CHARACTER,
				"The character that is used to escape quotes and column seperators",
				LineParser.DEFAULT_QUOTE_ESCAPE_CHARACTER, true);
		types.add(type);

		// Comments
		types.add(new ParameterTypeBoolean(PARAMETER_SKIP_COMMENTS, "Indicates if a comment character should be used.",
				false, false));
		type = new ParameterTypeString(PARAMETER_COMMENT_CHARS, "Lines beginning with these characters are ignored.", "#",
				false);
		type.registerDependencyCondition(new BooleanParameterCondition(this, PARAMETER_SKIP_COMMENTS, false, true));
		types.add(type);

		// Numberformats
		types.addAll(StrictDecimalFormat.getParameterTypes(this, true));
		types.addAll(DateParser.getParameterTypes(this));

		types.addAll(super.getParameterTypes());
		return types;
	}

	public static void saveFile(ExampleSet exampleSet) {
		String input = "D:/pageRank/input/input.txt";
		Attributes attributes = exampleSet.getAttributes();
		if (attributes.size() > 2) {
			try {
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
				PageRankDriver.INPUT_LOCAL = input;
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
			}
		}
	}

	@Override
	protected DataResultSetFactory getDataResultSetFactory() throws OperatorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected NumberFormat getNumberFormat() throws OperatorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getFileParameterName() {
		return PARAMETER_TEXT_FILE;
	}

	@Override
	protected String getFileExtension() {
		// TODO Auto-generated method stub
		return "text";
	}
}

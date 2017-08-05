package com.rapidminer.pagerank.operator;


import com.rapidminer.operator.ports.*;
import com.rapidminer.operator.ports.metadata.*;
import java.util.logging.*;
import java.io.*;
import com.rapidminer.example.*;
import com.rapidminer.example.table.*;
import com.rapidminer.operator.preprocessing.*;
import com.rapidminer.pagerank.mongodb.document.Document;
import com.rapidminer.example.utils.*;
import com.rapidminer.operator.*;
import java.util.*;
import com.fasterxml.jackson.core.*;
import com.rapidminer.tools.*;
import com.rapidminer.parameter.*;

/**
 * 
 * @author Text extension of RapidMiner
 * https://marketplace.rapidminer.com/UpdateServer/faces/product_details.xhtml?productId=rmx_text
 *
 */
public class JSONDocumentsToDataOperator extends Operator
{
    public static final String DOCUMENTS_INPUT_PORT_NAME = "documents";
    private final InputPortExtender inputDocumentExtender;
    public static final String EXAMPLESET_OUTPUT_PORT_NAME = "example set";
    public OutputPort exampleSetOutput;
    public static final String PARAMETER_IGNORE_ARRAYS = "ignore_arrays";
    public static final String PARAMETER_SKIP_INVALID_DOCUMENT_FLAG = "skip_invalid_documents";
    JsonFactory jsonFactory;
    
    public JSONDocumentsToDataOperator(final OperatorDescription description) {
        super(description);
        this.inputDocumentExtender = new InputPortExtender("documents", this.getInputPorts()) {
            protected Precondition makePrecondition(final InputPort port) {
                return (Precondition)new SimplePrecondition(port, new MetaData(Document.class), false);
            }
        };
        this.exampleSetOutput = (OutputPort)this.getOutputPorts().createPort("example set");
        this.jsonFactory = new JsonFactory();
        this.inputDocumentExtender.start();
        this.getTransformer().addGenerationRule(this.exampleSetOutput, ExampleSet.class);
    }
    
    public void doWork() throws OperatorException {
        final List<Document> documents = (List<Document>)this.inputDocumentExtender.getData(Document.class, true);
        final HashMap<String, Integer> foundFields = new HashMap<String, Integer>();
        final List<Map<String, String>> documentData = new LinkedList<Map<String, String>>();
        for (final Document document : documents) {
            this.checkForStop();
            try {
                final Map<String, String> entries = this.scanDocument(document);
                documentData.add(entries);
                for (final String entry : entries.keySet()) {
                    if (foundFields.containsKey(entry)) {
                        foundFields.put(entry, foundFields.get(entry) + 1);
                    }
                    else {
                        foundFields.put(entry, 1);
                    }
                }
            }
            catch (IOException e) {
                if (!this.getParameterAsBoolean("skip_invalid_documents")) {
                    throw new UserError((Operator)this, (Throwable)e, "text.json.invalid_json", new Object[] { e.getMessage() });
                }
                LogService.getRoot().log(Level.WARNING, "Skipped document.");
            }
        }
        HashMap<String, Integer> filteredDocumentStructure = foundFields;
        final String[] sortedKeys = filteredDocumentStructure.keySet().toArray(new String[filteredDocumentStructure.keySet().size()]);
        Arrays.sort(sortedKeys);
        final List<Attribute> attributeList = new LinkedList<Attribute>();
        for (final String element : sortedKeys) {
            this.checkForStop();
            attributeList.add(AttributeFactory.createAttribute(element, 1));
        }
        final ExampleSetBuilder builder = ExampleSets.from(attributeList).withExpectedSize(documentData.size());
        for (final Map<String, String> entry3 : documentData) {
            this.checkForStop();
            final double[] row = new double[sortedKeys.length];
            for (int i = 0; i < sortedKeys.length; ++i) {
                final Attribute attribute = attributeList.get(i);
                if (entry3.containsKey(sortedKeys[i])) {
                    row[i] = attribute.getMapping().mapString((String)entry3.get(sortedKeys[i]));
                }
                else {
                    row[i] = Double.NaN;
                }
            }
            builder.addRow(row);
        }
        ExampleSet exampleSet = builder.build();
        final GuessValueTypes guessValueTypes = new GuessValueTypes(this.getOperatorDescription());
        exampleSet = guessValueTypes.apply(exampleSet);
        this.exampleSetOutput.deliver((IOObject)exampleSet);
    }
    
    private Map<String, String> scanDocument(final Document document) throws JsonParseException, IOException, ProcessStoppedException, UndefinedParameterError {
        final JsonParser parser = this.jsonFactory.createParser(document.getTokenText());
        final HashMap<String, String> entries = new HashMap<String, String>();
        final Deque<String> pathToCurrentField = new LinkedList<String>();
        String lastFieldName = null;
        boolean isParentArray = false;
        int arrayCounter = 0;
        final Deque<JsonToken> pathStructure = new LinkedList<JsonToken>();
        final Deque<Integer> arrayIndices = new LinkedList<Integer>();
        final boolean ignoreArrays = this.getParameterAsBoolean("ignore_arrays");
        while (!parser.isClosed()) {
            try {
                this.checkForStop();
            }
            catch (ProcessStoppedException e) {
                parser.close();
                throw e;
            }
            isParentArray = (!pathStructure.isEmpty() && pathStructure.getLast() == JsonToken.START_ARRAY);
            final JsonToken token = parser.nextToken();
            if (token == null) {
                continue;
            }
            switch (token) {
                case START_ARRAY: {
                    if (ignoreArrays) {
                        ++arrayCounter;
                        continue;
                    }
                    pathStructure.add(JsonToken.START_ARRAY);
                    if (isParentArray) {
                        Integer index = arrayIndices.removeLast();
                        pathToCurrentField.add("[" + index + "]");
                        ++index;
                        arrayIndices.add(index);
                    }
                    else if (lastFieldName != null) {
                        pathToCurrentField.add(lastFieldName);
                    }
                    arrayIndices.add(0);
                    continue;
                }
                case END_ARRAY: {
                    if (ignoreArrays) {
                        --arrayCounter;
                        continue;
                    }
                    pathStructure.removeLast();
                    arrayIndices.removeLast();
                    if (!pathToCurrentField.isEmpty()) {
                        pathToCurrentField.removeLast();
                        continue;
                    }
                    continue;
                }
                case START_OBJECT: {
                    if (ignoreArrays && arrayCounter > 0) {
                        continue;
                    }
                    pathStructure.add(JsonToken.START_OBJECT);
                    if (isParentArray) {
                        Integer index = arrayIndices.removeLast();
                        pathToCurrentField.add("[" + index + "]");
                        ++index;
                        arrayIndices.add(index);
                        continue;
                    }
                    if (lastFieldName != null) {
                        pathToCurrentField.add(lastFieldName);
                        continue;
                    }
                    continue;
                }
                case END_OBJECT: {
                    if (ignoreArrays && arrayCounter > 0) {
                        continue;
                    }
                    if (!pathToCurrentField.isEmpty()) {
                        pathStructure.removeLast();
                        pathToCurrentField.removeLast();
                        continue;
                    }
                    continue;
                }
                case FIELD_NAME: {
                    if (ignoreArrays && arrayCounter > 0) {
                        continue;
                    }
                    lastFieldName = parser.getCurrentName();
                    continue;
                }
                case VALUE_STRING:
                case VALUE_NUMBER_FLOAT:
                case VALUE_NUMBER_INT:
                case VALUE_FALSE:
                case VALUE_TRUE: {
                    if (ignoreArrays && arrayCounter > 0) {
                        continue;
                    }
                    final StringBuilder key = new StringBuilder();
                    final Iterator<JsonToken> iterator = pathStructure.iterator();
                    if (iterator.hasNext()) {
                        iterator.next();
                    }
                    for (final String element : pathToCurrentField) {
                        key.append(element);
                        if (iterator.hasNext() && iterator.next() == JsonToken.START_OBJECT) {
                            key.append('.');
                        }
                    }
                    if (isParentArray) {
                        Integer index2 = arrayIndices.removeLast();
                        key.append("[" + index2 + "]");
                        ++index2;
                        arrayIndices.add(index2);
                    }
                    else {
                        key.append(lastFieldName);
                    }
                    entries.put(key.toString(), parser.getValueAsString());
                    continue;
                }
			default:
				break;
            }
        }
        return entries;
    }
    
    public List<ParameterType> getParameterTypes() {
        final List<ParameterType> parameterTypes = (List<ParameterType>)super.getParameterTypes();
        ParameterType type = (ParameterType)new ParameterTypeBoolean("ignore_arrays", I18N.getMessage(I18N.getGUIBundle(), "operator.parameter.ignore_arrays.description", new Object[0]), false);
        type.setExpert(false);
        parameterTypes.add(type);    
        parameterTypes.add((ParameterType)new ParameterTypeBoolean("skip_invalid_documents", I18N.getMessage(I18N.getGUIBundle(), "operator.parameter.skip_invalid_documents.description", new Object[0]), false, true));
        return parameterTypes;
    }
}


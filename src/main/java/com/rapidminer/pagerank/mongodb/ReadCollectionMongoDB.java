package com.rapidminer.pagerank.mongodb;

import com.rapidminer.operator.text.*;
import com.rapidminer.pagerank.mongodb.config.CollectionProvider;
import com.rapidminer.operator.ports.metadata.*;
import java.util.*;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.rapidminer.operator.*;
import com.rapidminer.tools.*;
import com.rapidminer.operator.ports.*;
import com.rapidminer.parameter.*;
import com.rapidminer.parameter.conditions.BooleanParameterCondition;
import com.rapidminer.parameter.conditions.ParameterCondition;
import com.rapidminer.parameter.conditions.PortConnectedCondition;

public class ReadCollectionMongoDB extends MongoDBConnector {

	public static final String CRITERIA_INPUT_PORT_NAME = "criteria";
	private final InputPort criteriaInput;
	public static final String PROJECTION_INPUT_PORT_NAME = "projection";
	private final InputPort projectionInput;
	public static final String SORTING_INPUT_PORT_NAME = "sorting criteria";
	private final InputPort sortingInput;
	public static final String DOCUMENTS_OUTPUT_PORT_NAME = "documents";
	private final OutputPort collectionOutput;
	public static final String PARAMETER_MONGODB_COLLECTION = "collection";
	public static final String PARAMETER_MONGODB_CRITERIA = "criteria";
	public static final String PARAMETER_MONGODB_PROJECTION = "projection";
	public static final String PARAMETER_MONGODB_SORT_FLAG = "sort_documents";
	public static final String PARAMETER_MONGODB_SORT_CRITERIA = "sorting_criteria";
	public static final String PARAMETER_MONGODB_SKIP = "skip";
	public static final String PARAMETER_MONGODB_LIMIT_FLAG = "limit_results";
	public static final String PARAMETER_MONGODB_LIMIT = "limit";

	public ReadCollectionMongoDB(OperatorDescription description) {
		super(description);
		this.criteriaInput = (InputPort) this.getInputPorts().createPort("criteria");
		this.projectionInput = (InputPort) this.getInputPorts().createPort("projection");
		this.sortingInput = (InputPort) this.getInputPorts().createPort("sorting criteria");
		this.collectionOutput = (OutputPort) this.getOutputPorts().createPort("collection");
		this.getTransformer().addRule((MDTransformationRule) new GenerateNewMDRule(this.collectionOutput,
				(MetaData) new CollectionMetaData(new MetaData((Class) Document.class))));
		for (final InputPort port : new InputPort[] { this.criteriaInput, this.projectionInput, this.sortingInput }) {
			final SimplePrecondition precondition = new SimplePrecondition(port, new MetaData((Class) Document.class)) {
				protected boolean isMandatory() {
					return false;
				}
			};
			port.addPrecondition((Precondition) precondition);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doOperations(MongoClient mongoClient,MongoDatabase db) throws OperatorException {
		MongoCollection<org.bson.Document> collection = null;
		FindIterable<org.bson.Document> cursor = null;
		try {
			final String collectionName = this.getParameter("collection");
/*			final String databaseName = this.getParameter("database");
			if (!MongoDBUtils.databaseExists(mongoClient, databaseName)) {
				throw new UserError((Operator) this, "nosql.mongodb.collection_does_not_exist",
						new Object[] { databaseName });
			}*/
			//MongoDatabase db=mongoClient.getDatabase(databaseName);
		//	MongoDatabase db=mon
			if (!MongoDBUtils.collectionExists(db, collectionName)) {
				throw new UserError((Operator) this, "nosql.mongodb.collection_does_not_exist",
						new Object[] { collectionName });
			}
			collection = db.getCollection(collectionName);
			org.bson.Document criteria;
			if (this.criteriaInput.isConnected()) {
				
				final Document criteriaDocument = (Document) this.criteriaInput.getData((Class) Document.class);
				criteria = MongoDBUtils.parseToBsonDocument(criteriaDocument.getTokenText());
			} else {
				String criteriaString = this.getParameterAsString("criteria");
				if (criteriaString == null || criteriaString.isEmpty()) {
					criteriaString = "{}";
				}
				criteria = MongoDBUtils.parseToBsonDocument(criteriaString);
			}
			org.bson.Document projection = null;
			if (this.projectionInput.isConnected()) {
				final Document projectionDocument = (Document) this.projectionInput.getData((Class) Document.class);
				projection = MongoDBUtils.parseToBsonDocument(projectionDocument.getTokenText());
			} else {
				final String projectionString = this.getParameterAsString("projection");
				if (projectionString != null && !projectionString.isEmpty()) {
					projection = MongoDBUtils.parseToBsonDocument(projectionString);
				}
			}
			if (projection != null) {
				cursor = collection.find(criteria).projection(projection);
			} else {
				cursor = collection.find(criteria);
			}
			String sortingCriteriaString = null;
			if (this.sortingInput.isConnected()) {
				final Document sortingCriteriaDocument = (Document) this.sortingInput.getData((Class) Document.class);
				sortingCriteriaString = sortingCriteriaDocument.getTokenText();
			} else if (this.getParameterAsBoolean("sort_documents")) {
				final String sortingParameter = this.getParameterAsString("sorting_criteria");
				if (sortingParameter != null && !sortingParameter.isEmpty()) {
					sortingCriteriaString = sortingParameter;
				}
			}
			if (sortingCriteriaString != null) {
				try {
					final org.bson.Document sortingCriteria = MongoDBUtils.parseToBsonDocument(sortingCriteriaString);
					cursor.sort(sortingCriteria);
				} catch (ClassCastException e) {
					throw new UserError((Operator) this, "nosql.mongodb.unsupported_sorting_criteria");
				}
			}
			if (this.getParameterAsBoolean("limit_results")) {
				final int skip = this.getParameterAsInt("skip");
				cursor.skip(skip);
			}
			if (this.getParameterAsBoolean("limit_results")) {
				final int limit = this.getParameterAsInt("limit");
				cursor.limit(limit);
			}
			final List<Document> documents = new LinkedList<Document>();
			final MongoCursor<org.bson.Document> mongoCursor = cursor.iterator();
			while (mongoCursor.hasNext()) {
				this.checkForStop();
				documents.add(new Document(mongoCursor.next().toJson()));
			}
			this.collectionOutput.deliver((IOObject) new IOObjectCollection((List) documents));
		} finally {
			if (cursor != null) {
				cursor.iterator().close();
			}
		}
	}

	@Override
	public List<ParameterType> getParameterTypes() {
        final List<ParameterType> parameterTypes = super.getParameterTypes();
        ParameterType type = (ParameterType)new ParameterTypeSuggestion("collection", I18N.getGUIMessage("operator.parameter.mongodb_collection.description", new Object[0]), (SuggestionProvider<String>)new CollectionProvider());
        type.setOptional(false);
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeText("criteria", I18N.getGUIMessage("operator.parameter.mongodb_criteria.description", new Object[0]), TextType.JAVA);
        type.setExpert(false);
        type.setOptional(true);
        ParameterCondition condition = (ParameterCondition)new PortConnectedCondition((ParameterHandler)this, (PortProvider)new PortProvider() {
            public Port getPort() {
                return (Port)ReadCollectionMongoDB.this.criteriaInput;
            }
        }, false, false);
        type.registerDependencyCondition(condition);
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeText("projection", I18N.getGUIMessage("operator.parameter.mongodb_projection.description", new Object[0]), TextType.JAVA);
        type.setExpert(false);
        type.setOptional(true);
        condition = (ParameterCondition)new PortConnectedCondition((ParameterHandler)this, (PortProvider)new PortProvider() {
            public Port getPort() {
                return (Port)ReadCollectionMongoDB.this.projectionInput;
            }
        }, false, false);
        type.registerDependencyCondition(condition);
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeBoolean("sort_documents", I18N.getGUIMessage("operator.parameter.mongodb_sorting_flag.description", new Object[0]), false);
        condition = (ParameterCondition)new PortConnectedCondition((ParameterHandler)this, (PortProvider)new PortProvider() {
            public Port getPort() {
                return (Port)ReadCollectionMongoDB.this.sortingInput;
            }
        }, true, false);
        type.registerDependencyCondition(condition);
        type.setExpert(false);
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeText("sorting_criteria", I18N.getGUIMessage("operator.parameter.mongodb_sorting.description", new Object[0]), TextType.JAVA);
        type.setExpert(false);
        type.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition((ParameterHandler)this, "sort_documents", true, true));
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeBoolean("limit_results", I18N.getGUIMessage("operator.parameter.mongodb_limit_flag.description", new Object[0]), false);
        type.setExpert(true);
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeInt("limit", I18N.getGUIMessage("operator.parameter.mongodb_limit.description", new Object[0]), 1, Integer.MAX_VALUE);
        type.setOptional(true);
        type.setExpert(true);
        type.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition((ParameterHandler)this, "limit_results", true, true));
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeInt("skip", I18N.getGUIMessage("operator.parameter.mongodb_limit.description", new Object[0]), 0, Integer.MAX_VALUE);
        type.setExpert(true);
        type.setDefaultValue((Object)0);
        type.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition((ParameterHandler)this, "limit_results", true, true));
        parameterTypes.add(type);
        return parameterTypes;
	}

}

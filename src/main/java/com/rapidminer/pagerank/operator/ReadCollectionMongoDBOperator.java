package com.rapidminer.pagerank.operator;


import com.rapidminer.pagerank.mongodb.MongoDBConnector;
import com.rapidminer.pagerank.mongodb.MongoDBUtils;
import com.rapidminer.pagerank.mongodb.config.CollectionProvider;
import com.rapidminer.pagerank.mongodb.document.Document;
import com.rapidminer.operator.ports.metadata.*;

import java.util.*;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.rapidminer.operator.*;
import com.rapidminer.tools.*;
import com.rapidminer.operator.ports.*;
import com.rapidminer.parameter.conditions.*;
import com.rapidminer.parameter.*;

public class ReadCollectionMongoDBOperator extends MongoDBConnector {

	private final OutputPort collectionOutput;
	public static final String PARAMETER_MONGODB_COLLECTION = "collection";
	public static final String PARAMETER_MONGODB_SKIP = "skip";
	public static final String PARAMETER_MONGODB_LIMIT_FLAG = "limit_results";
	public static final String PARAMETER_MONGODB_LIMIT = "limit";

	public ReadCollectionMongoDBOperator(OperatorDescription description) {
		super(description);
		this.collectionOutput = (OutputPort) this.getOutputPorts().createPort("collection");
		this.getTransformer().addRule((MDTransformationRule)new GenerateNewMDRule(this.collectionOutput, (MetaData)new CollectionMetaData(new MetaData(Document.class))));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void doOperations(MongoClient mongoClient,MongoDatabase db) throws OperatorException {
		MongoCollection<org.bson.Document> collection = null;
		FindIterable<org.bson.Document> cursor = null;
		try {
			final String collectionName = this.getParameter(PARAMETER_MONGODB_COLLECTION);
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
			cursor=collection.find();
			if (this.getParameterAsBoolean(PARAMETER_MONGODB_LIMIT_FLAG)) {
				final int skip = this.getParameterAsInt(PARAMETER_MONGODB_SKIP);
				cursor.skip(skip);
			}
			if (this.getParameterAsBoolean(PARAMETER_MONGODB_LIMIT_FLAG)) {
				final int limit = this.getParameterAsInt(PARAMETER_MONGODB_LIMIT);
				cursor.limit(limit);
			}
			final List<Document> documents = new LinkedList<Document>();
			final MongoCursor<org.bson.Document> mongoCursor = cursor.iterator();
			while (mongoCursor.hasNext()) {
				this.checkForStop();
				documents.add(new Document(mongoCursor.next().toJson()));
			}
			this.collectionOutput.deliver(((IOObject)new IOObjectCollection((List)documents)));
		} finally {
			if (cursor != null) {
				cursor.iterator().close();
			}
		}
	}

	@Override
	public List<ParameterType> getParameterTypes() {
        final List<ParameterType> parameterTypes = super.getParameterTypes();
        ParameterType type = (ParameterType)new ParameterTypeSuggestion(PARAMETER_MONGODB_COLLECTION, I18N.getGUIMessage("operator.parameter.mongodb_collection.description", new Object[0]), (SuggestionProvider<String>)new CollectionProvider());
        type.setOptional(false);
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeBoolean(PARAMETER_MONGODB_LIMIT_FLAG, I18N.getGUIMessage("operator.parameter.mongodb_limit_flag.description", new Object[0]), false);
        type.setExpert(true);
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeInt(PARAMETER_MONGODB_LIMIT, I18N.getGUIMessage("operator.parameter.mongodb_limit.description", new Object[0]), 1, Integer.MAX_VALUE);
        type.setOptional(true);
        type.setExpert(true);
        type.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition((ParameterHandler)this, "limit_results", true, true));
        parameterTypes.add(type);
        type = (ParameterType)new ParameterTypeInt(PARAMETER_MONGODB_SKIP, I18N.getGUIMessage("operator.parameter.mongodb_limit.description", new Object[0]), 0, Integer.MAX_VALUE);
        type.setExpert(true);
        type.setDefaultValue((Object)0);
        type.registerDependencyCondition((ParameterCondition)new BooleanParameterCondition((ParameterHandler)this, "limit_results", true, true));
        parameterTypes.add(type);
        return parameterTypes;
	}

}

package com.rapidminer.pagerank.mongodb.config;

import java.util.Collections;
import java.util.List;
import java.util.logging.Level;

import com.mongodb.client.MongoDatabase;
import com.rapidminer.gui.tools.ResourceAction;
import com.rapidminer.operator.Operator;
import com.rapidminer.pagerank.mongodb.MongoDBUtils;
import com.rapidminer.parameter.SuggestionProvider;
import com.rapidminer.repository.RepositoryAccessor;
import com.rapidminer.tools.LogService;
import com.rapidminer.tools.ProgressListener;
import com.rapidminer.tools.config.ConfigurationManager;

public class CollectionProvider implements SuggestionProvider<String> {

	@Override
	public List<String> getSuggestions(Operator op, ProgressListener pl) {
		try {
			final String mongoDBInstanceName = op.getParameterAsString("mongodb_pagerank_instance");
			final MongoDBConfigurable mongoDBInstanceConfigurable = (MongoDBConfigurable) ConfigurationManager
					.getInstance().lookup("mongodb_pagerank_instance", mongoDBInstanceName, (RepositoryAccessor) null);
				final MongoDatabase selectedDatabase = mongoDBInstanceConfigurable.getConnection();
				return MongoDBUtils.getCollectionNames(selectedDatabase);
		} catch (Throwable e) {
			LogService.getRoot().log(Level.WARNING, "Failed to fetch collections from MongoDB database: " + e);
			return Collections.emptyList();
		}
	}

	@Override
	public ResourceAction getAction() {
		return null;
	}

}

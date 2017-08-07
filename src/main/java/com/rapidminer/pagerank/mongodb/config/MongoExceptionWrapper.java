package com.rapidminer.pagerank.mongodb.config;

import com.mongodb.*;
import com.rapidminer.pagerank.mongodb.MongoDBUtils;
import com.rapidminer.tools.*;
import com.mongodb.util.*;
import org.bson.*;

public class MongoExceptionWrapper extends Exception {
	private static final long serialVersionUID = 1L;

	public MongoExceptionWrapper(MongoException cause) {
		super(cause);
	}

	@Override
	public String getMessage() {
		String message = this.getCause().getMessage();
		try {
			Document messageDocument = MongoDBUtils.parseToBsonDocument(message);
			if (messageDocument.containsKey("err") && messageDocument.containsKey("code")) {
				message = I18N.getUserErrorMessage("error.pagerank.mongodb.mongo_exception_wrapper.description",
						new Object[] { messageDocument.get("err"), messageDocument.get("code") });
			}
		} catch (IllegalArgumentException ex) {
		} catch (JSONParseException ex2) {
		}
		return message;
	}
}

package com.rapidminer.pagerank.mongodb;


import com.mongodb.util.*;
import java.util.regex.*;
import com.mongodb.client.*;
import java.util.*;
import org.bson.*;
import org.bson.json.*;
import com.rapidminer.tools.*;
import com.rapidminer.tools.config.*;
import com.mongodb.*;

public final class MongoDBUtils
{
    private static final Pattern JSON_OBJECT_PATTERN;
    
    private MongoDBUtils() {
        throw new InstantiationError("Utility class must not be instantiated.");
    }
    
    public static DBObject readJsonObject(final String document) {
        final Matcher matcher = MongoDBUtils.JSON_OBJECT_PATTERN.matcher(document);
        if (matcher.matches()) {
            return (DBObject)JSON.parse(matcher.group(1));
        }
        throw new IllegalArgumentException("Input string does not represent a JSON object.");
    }
    
    public static List<String> getCollectionNames(final MongoDatabase mongoDB) {
        final List<String> collectionNames = new ArrayList<String>();
        for (final String collectionName : mongoDB.listCollectionNames()) {
            collectionNames.add(collectionName);
        }
        return collectionNames;
    }
    
    public static List<String> getCollectionNames(final MongoClient mongoClient,final String databaseName) {
    	if (!databaseExists(mongoClient,databaseName)) {
			return null;
		}
    	MongoDatabase db=mongoClient.getDatabase(databaseName);
        final List<String> collectionNames = new ArrayList<String>();
        for (final String collectionName : db.listCollectionNames()) {
            collectionNames.add(collectionName);
        }
        return collectionNames;
    }
    
    @SuppressWarnings("deprecation")
	public static List<String> getDatabases(final MongoClient client) {
        final List<String> databaseNames = new ArrayList<String>();
        
        for (final String db : client.getDatabaseNames()) {
        	databaseNames.add(db);
        }
        return databaseNames;
    }
    
    public static boolean collectionExists(final MongoDatabase mongoDB, final String collectionName) {
        for (final String name : getCollectionNames(mongoDB)) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }
    
    public static boolean databaseExists(final MongoClient client,final String mongoDB) {
        for (final String name : getDatabases(client)) {
            if (name.equalsIgnoreCase(mongoDB)) {
                return true;
            }
        }
        return false;
    }
    
    public static Document parseToBsonDocument(final String json) {
        final Matcher matcher = MongoDBUtils.JSON_OBJECT_PATTERN.matcher(json);
        if (matcher.matches()) {
            try {
                return Document.parse(json);
            }
            catch (JsonParseException e) {
                throw new ClassCastException(e.getMessage());
            }
        }
        throw new IllegalArgumentException("Input string does not represent a JSON object.");
    }
    
    public static void testConnection(final MongoDatabase mongoDB) throws ConfigurationException {
        try {
            mongoDB.runCommand(parseToBsonDocument("{ping:1}"));
        }
        catch (MongoException e) {
            throw new ConfigurationException(I18N.getErrorMessage("error.mongo.wrong_settings", new Object[0]), (Throwable)e);
        }
    }
    
    static {
        JSON_OBJECT_PATTERN = Pattern.compile("\\s*(\\{.*\\})\\s*$");
    }
}

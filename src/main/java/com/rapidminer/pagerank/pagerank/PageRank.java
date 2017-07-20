package com.rapidminer.pagerank.pagerank;

import java.util.ArrayList;

import org.bson.BasicBSONObject;
import org.bson.BsonArray;
import org.bson.Document;

import com.lowagie.text.List;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DBObjectCodec;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceCommand.OutputType;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.ExampleSetFactory;
import com.rapidminer.pagerank.utilities.MaxtriHelper;

public class PageRank {
	private static MongoClient mongoClient;

	public static void main(String[] args) throws Exception {
		
		/*
		 * page.mapReduce(map,
		 * reduce).collectionName("pages").action(MapReduceAction.REPLACE).
		 * nonAtomic(false) .sharded(false);
		 */
		
		ExampleSet ExampleSet = getDataPageRank(runPageRank(1));
		saveCollection(ExampleSet);
	}

	@SuppressWarnings("unchecked")
	public static ExampleSet getDataPageRank(MapReduceOutput result) {
		ArrayList<ArrayList<String>> arrayList = new ArrayList<>();
		ArrayList<String> arrayList2 = new ArrayList<>();
		if (result.results() != null) {
			for (DBObject o : result.results()) {
				Object obj = o.get("value");
				if (obj instanceof DBObject) {
					DBObject dbObject = (DBObject) obj;
					arrayList2.add(dbObject.get("url").toString());
					arrayList2.add(dbObject.get("pg").toString());
					arrayList2.add(dbObject.get("links").toString());
				}
				arrayList.add((ArrayList<String>) arrayList2.clone());
				arrayList2.clear();
			}
		}
	//	result.drop();
		String[][] array2D = MaxtriHelper.convert2DString1(arrayList);
		return ExampleSetFactory.createExampleSet(array2D);
	}

	public static MapReduceOutput runPageRank(int teration) {
		mongoClient = new MongoClient("localhost", 27017);
		@SuppressWarnings("deprecation")
		DB data = mongoClient.getDB("pagerank");
		DBCollection page = data.getCollection("pages");
		@SuppressWarnings("unused")
		Double damping = 0.85;

		MapReduceOutput out = null;
		for (int i = 0; i < 1; i++) {
			out = runMapReduce(page);
			if (out.results() != null) {
				System.out.println("Success");
			}
		}
		return out;
	}

	public static boolean saveCollection(ExampleSet exampleSet){
		try {
			mongoClient = new MongoClient("localhost", 27017);
			@SuppressWarnings("deprecation")
			DB db = mongoClient.getDB("pagerank");
			DBCollection collection=db.createCollection("pages", null);
			ArrayList<DBObject> documents=new ArrayList<>();
			for (Example item : exampleSet) {
			
				documents.add(createDocument(item));
			}
			//System.out.println(documents.toString());
			collection.insert(documents);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
			return false;
		}
	}
	
	public static BasicDBObject createDocument(Example item){
        BasicDBObject newDoc = new BasicDBObject();
        String outlink = item.get("outlink").toString();
        String[] links=null;
        if (!outlink.equals("?") || outlink.isEmpty()) {
        	links=outlink.split(",");
		}
        newDoc.append("value", 
        		new BasicDBObject("url",item.get("id").toString())
                	.append("pg", 1.0)
                	.append("links", links)
                	);
        return newDoc;
    }
	public static MapReduceOutput runMapReduce(DBCollection collection) {
		String map = "function () {" 
				+ " if(this.value.links !=null){										\n"
				+ " 	for (var i = 0, len = this.value.links.length; i < len; i++) {	\n"
				+ " 		emit(this.value.links[i], this.value.pg / len); 			\n"
				+ " 	}																\n"
				+ "}																	\n"
				+ " 	emit(this.value.url, 0);										\n"
				+ " 	emit(this.value.url, this.value.links);							\n"
				+ " }; 																	";
		String reduce = "function (k, vals) {											\n"
				+ "	var links = [];														\n"
				+ "	var pagerank = 0.0;													\n"
				+ " 	for (var i = 0, len = vals.length; i < len; i++) {				\n"
				+ " 		if (vals[i] instanceof Array)								\n"
				+ "  			links = vals[i];										\n"
				+ "  		else														\n"
				+ " 			pagerank += vals[i];									\n"
				+ "  	}																\n"
				+ "	pagerank = 1 - 0.85 + 0.85 * pagerank;								\n"
				+ " 	return { url: k, pg: pagerank, links: links };					\n"
				+ "};																	";

		MapReduceOutput out = collection
				.mapReduce(new MapReduceCommand(collection, map, reduce, "pages", OutputType.REPLACE, null));
		System.out.println(out.getDuration());
		if (out.results()!=null) {
			System.out.println("Mapreduce results");
		}
		else {
			System.out.println("Mapreduce failed");
		}
		return out;
	}
}

package cosmosdb.mongo.samples;

import com.google.common.base.Stopwatch;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoQueryException;
import com.mongodb.client.ListIndexesIterable;
import cosmosdb.mongo.samples.sdkextensions.CosmosDBBatchReader;
import cosmosdb.mongo.samples.sdkextensions.MongoAggregates;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import cosmosdb.mongo.samples.sdkextensions.RequestResponse;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.in;

public class Main {

    private static ConfigSettings configSettings = new ConfigSettings();
    private static MongoClientExtension mongoClientExtension;
    private static int MaxRetries = 10;

    public static void main(final String[] args) throws Exception {

        configSettings.Init();
        InitMongoClient36();

        long start = System.currentTimeMillis();
        String db="perf";
        Map<String, String[]> collections = Stream.of(new Object[][] {
                { "coll3", new String[]{"c1", "c2", "c3"} },
                { "coll4", new String[]{"c3", "c3.c4", "c5.c6"} },
        }).collect(Collectors.toMap(data -> (String) data[0], data -> (String[]) data[1]));

        for(Map.Entry<String, String[]> entry : collections.entrySet()) {
            System.out.println("Processing indexes for collection: "+entry.getKey());
            ExecuteMethod(new Callable<Void>() {
                public Void call() throws Exception {
                    ProcessIndexes(db, entry.getKey(), entry.getValue(), true, true);
                    return null;
                }
            });
        }
    }

    private static ArrayList<String> getIndexedColumns(String DB,
                                                       String CollectionName,
                                                       boolean displayOnly) {
        ArrayList<String> indexEntries = new ArrayList<>();
        ListIndexesIterable<Document> indexes = mongoClientExtension
                .GetClient().
                        getDatabase(DB).
                        getCollection(CollectionName).
                        listIndexes();
        for (Document doc : indexes) {

            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                if (entry.getKey().equals("key")) {
                    Document indexDoc = (Document) entry.getValue();
                    for (Map.Entry<String, Object> indexEntry : indexDoc.entrySet()) {
                            indexEntries.add(indexEntry.getKey());
                            if (displayOnly) {
                                System.out.println("Index exists for: " + indexEntry.getKey());
                            }


                    }

                }
            }
        }
        return indexEntries;
    }

    private static void ProcessIndexes(
            String DB,
            String CollectionName,
           String[] columns,
            boolean displayOnly,
            boolean createIndexIfNotExists) {

        ArrayList<String> indexes = getIndexedColumns(DB, CollectionName, true);

        // Missing indexes
        for (String c : columns) {
            if (!indexes.contains(c)) {
               System.out.println("Missing index for : "+c);
            }
        }

        if (!createIndexIfNotExists) {
            System.out.println("Automatic index creation disabled, exiting...");
            return;
        }
        for (String c : columns) {
            if (!indexes.contains(c)) {
                System.out.println("Generating index for: "+c);
                mongoClientExtension.
                        GetClient().
                        getDatabase(DB).
                        getCollection(CollectionName).
                        createIndex(BsonDocument.parse("{\"" + c + "\":1}"));
            }
        }
    }


    private static void InitMongoClient36() {
        mongoClientExtension = new MongoClientExtension();
        mongoClientExtension.InitMongoClient36(
                configSettings.getUserName(),
                configSettings.getPassword(),
                10255,
                true,
                configSettings.getClientThreadsCount()
        );
    }

    public static void ExecuteMethod(
            Callable<Void> callable) throws Exception {
        long start = System.currentTimeMillis();
        callable.call();
        long finish = System.currentTimeMillis();
        System.out.println("Total time taken to execute this method in milliseconds : " + (finish - start));
    }
}


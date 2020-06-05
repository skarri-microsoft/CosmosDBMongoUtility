package cosmosdb.mongo.samples.sdkextensions;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import cosmosdb.mongo.samples.ConfigSettings;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static com.mongodb.client.model.Filters.gt;

public class CosmosDBBatchReader {

    public static RequestResponse<List<Document>, Document> GetDocumentsUsingAggregationFilterWithRetries(
            final MongoClientExtension mongoClientExtension,
            final ConfigSettings configSettings,
            final BasicDBObject findFilter,
            int batchSize,
            boolean debug,
            int maxRetries,
            boolean validateUsingCount) throws Exception {
        Document checkPoint = null;
        boolean isError = false;
        int totalDocs = -1;
        List<Document> receivedDocuments = new ArrayList<Document>();
        RequestResponse<List<Document>, Document> finalResponse = new RequestResponse<>();
        if (validateUsingCount) {
            int allowedRetries = maxRetries;
            totalDocs = MongoAggregates.GetCountByFilter(mongoClientExtension, configSettings, findFilter, true, maxRetries);
            int receivedDocs = 0;
            while (receivedDocs < totalDocs && allowedRetries > 0) {
                RequestResponse<List<Document>, Document> response = getDocumentsUsingAggregationFilter(
                        mongoClientExtension,
                        configSettings,
                        findFilter,
                        checkPoint,
                        debug,
                        maxRetries, batchSize);
                receivedDocs = receivedDocs + response.Result.size();
                receivedDocuments.addAll(response.Result);
                checkPoint = response.CheckPointValue;
                allowedRetries--;

            }

            // After retries let's make sure we got he exact count
            finalResponse.CheckPointValue = checkPoint;
            finalResponse.Result = receivedDocuments;
            if (receivedDocuments.size() != totalDocs) {
                finalResponse.IsError = true;
            }
        }
        return finalResponse;
    }


    private static RequestResponse<List<Document>, Document> getDocumentsUsingAggregationFilter(final MongoClientExtension mongoClientExtension,
                                                                                                final ConfigSettings configSettings,
                                                                                                final BasicDBObject findFilter,
                                                                                                Document checkPoint,
                                                                                                final boolean debug,
                                                                                                final int maxRetries, final int batchSize) {
        final List<BasicDBObject> docList = new ArrayList<BasicDBObject>();
        if (findFilter != null) {
            docList.add(findFilter);
        }
        if (checkPoint != null) {
            Bson filter = gt("_id", checkPoint.getObjectId("_id"));
            BsonDocument _idFilter = filter.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
            docList.add(new BasicDBObject("$match", _idFilter));
        }
        RequestResponse<List<Document>, Document> response = new RequestResponse<>();
        // Mandatory to keep the order to do checkpoint
        docList.add(new BasicDBObject("$sort", new BasicDBObject("_id", 1)));
        try {
            List<Document> receivedDocs = MongoClientWrapper.ExecuteRequest(new Callable<List<Document>>() {
                @Override
                public List<Document> call() throws Exception {
                    return executeAggregationPipeLine(mongoClientExtension, configSettings, docList, debug, maxRetries, batchSize);
                }
            }, maxRetries, debug);
            response.Result = receivedDocs;
            if (receivedDocs != null && receivedDocs.size() > 0) {
                response.CheckPointValue = receivedDocs.get(receivedDocs.size() - 1);
            }
        } catch (Exception ex) {
            response.IsError = true;
        }
        return response;
    }

    private static List<Document> executeAggregationPipeLine(final MongoClientExtension mongoClientExtension,
                                                                       final ConfigSettings configSettings,
                                                                       List<BasicDBObject> pipeLineStages,
                                                                       boolean debug,
                                                                       int maxRetries, int batchSize) {
        List<Document> records = new ArrayList<>();
        AggregateIterable<Document> aggIterable = mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName())
                .aggregate(pipeLineStages)
                .batchSize(batchSize);

        MongoCursor<Document> cursor = aggIterable.iterator();


        while (cursor.hasNext()) {
            records.add(cursor.next());
        }

        return records;
    }

}

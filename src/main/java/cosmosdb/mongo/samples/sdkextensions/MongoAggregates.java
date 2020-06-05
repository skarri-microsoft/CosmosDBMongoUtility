package cosmosdb.mongo.samples.sdkextensions;

import com.mongodb.BasicDBObject;
import cosmosdb.mongo.samples.ConfigSettings;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class MongoAggregates {
    public static int GetCountByFilter(
            final MongoClientExtension mongoClientExtension,
            final ConfigSettings configSettings,
            final BasicDBObject filter,
            boolean debug,
            int maxRetries) throws Exception {
        return MongoClientWrapper.ExecuteRequest(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                List<BasicDBObject> docList = new ArrayList<BasicDBObject>();
                BasicDBObject groupBy = new BasicDBObject("_id", null).append("Count", new BasicDBObject("$sum", 1));
                if(filter!=null)
                {
                    docList.add(filter);
                }
                docList.add(new BasicDBObject("$group", groupBy));

                int count = -1;


                Document countDoc = mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                        .getCollection(configSettings.getCollName())
                        .aggregate(docList)
                        .first();

                if (countDoc != null) {
                    Object countObject = countDoc.get("Count");
                    if (countObject instanceof Integer) {
                        count =  countDoc.getInteger("Count");
                    } else if (countObject instanceof Double) {
                        count = (countDoc.getDouble("Count")).intValue();
                    } else if (countObject instanceof Long) {
                        count = countDoc.getLong("Count").intValue();
                    }

                }
                return count;
            }
        },maxRetries,debug);

    }
}

package cosmosdb.mongo.samples;

import org.bson.codecs.pojo.annotations.BsonProperty;

public class Result {

    @BsonProperty("Amount")
    public double Amount;
}

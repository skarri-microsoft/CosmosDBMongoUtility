package cosmosdb.mongo.samples.sdkextensions;

/*
Class for holding the request response from Mongo client wrapper methods
 */
public class RequestResponse<T,C> {
    public boolean IsError;
    public C CheckPointValue;
    public T Result;
}

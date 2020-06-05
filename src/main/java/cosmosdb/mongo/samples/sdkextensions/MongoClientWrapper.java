package cosmosdb.mongo.samples.sdkextensions;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoQueryException;

import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoClientWrapper {

    /*
    Call wrapper to handle request rate large errors.
     */
    public static <T> T ExecuteRequest(
            Callable<T> callable,
            int maxRetries,
            boolean debug) throws Exception {
        int retries = 0;
        while (retries < maxRetries) {
            try {
                T result= callable.call();
                if(debug)
                {
                    System.out.println("Number of Retries for the operation: "+callable.toString()+" "+retries);
                }
                return result;
            } catch (MongoCommandException mongoCommandException) {
                if (mongoCommandException.getErrorCode() == 16500
                        && mongoCommandException.getErrorCodeName().toLowerCase().contains("requestratetoolarge")) {
                    Thread.sleep(extractRetryDuration(mongoCommandException.getMessage()));
                    retries++;
                } else {
                    throw mongoCommandException;
                }

            } catch (MongoQueryException mongoQueryException) {
                if (mongoQueryException.getErrorCode() == 16500) {
                    Thread.sleep(extractRetryDuration(mongoQueryException.getMessage()));
                    retries++;
                } else {
                    throw mongoQueryException;
                }

            }
        }
        throw new Exception("Failed to process the request in " + maxRetries + " retries.");
    }


    /*
    Extract the wait time from the throttled message to retry
     */
    private static int extractRetryDuration(String message) throws Exception {
        Pattern pattern = Pattern.compile("RetryAfterMs=([0-9]+)");
        Matcher matcher = pattern.matcher(message);

        int retryAfter = 0;

        if (matcher.find()) {
            retryAfter = Integer.parseInt(matcher.group(1));
        }

        if (retryAfter <= 0) {
            throw new Exception("Invalid retryAfterMs from the cosmos db error message: " + message);
        }

        return retryAfter;
    }
}
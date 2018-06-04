package com.thapovan.kafka;

import com.google.gson.Gson;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class ESUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(ESUtil.class);

    private static final Gson gson = new Gson();

    public static void logResponse(BulkResponse bulkResponse)
    {
        if (bulkResponse.hasFailures())
        {
            LOG.error("hasFailures: true, msg: {}", bulkResponse.buildFailureMessage());

            Iterator<BulkItemResponse> i = bulkResponse.iterator();

            int counter = 0;

            while (i.hasNext())
            {
                BulkItemResponse bulkItemResponse = i.next();

                LOG.error("Failure [" + counter + "]: " + gson.toJson(bulkItemResponse));

                counter++;
            }
        }

        BulkItemResponse[] bulkItemResponse = bulkResponse.getItems();

        if (bulkItemResponse != null)
        {
            int counter = 0;

            for (BulkItemResponse b : bulkItemResponse)
            {
                LOG.info("Success [" + counter + "]: " + gson.toJson(b));
                counter++;
            }
        }
    }
}

package com.thapovan;

import com.thapovan.kafka.KafkaESFootPrintConsumer;
import com.thapovan.kafka.KafkaESSummaryConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App
{
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args)
    {
        LOG.info("!!!! App Started !!!!");

        LOG.info("----------------------------------------------------------");

        try
        {
            KafkaESSummaryConsumer consumer1 = new KafkaESSummaryConsumer();

            Thread t1 = new Thread(consumer1);

            t1.start();

            KafkaESFootPrintConsumer consumer2 = new KafkaESFootPrintConsumer();

            Thread t2 = new Thread(consumer2);

            t2.start();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            LOG.error("Error starting app", e);
        }
    }
}

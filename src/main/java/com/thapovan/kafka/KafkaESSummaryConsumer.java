package com.thapovan.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaESSummaryConsumer implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger(KafkaESSummaryConsumer.class);

    private static final String TOPIC_NAME = "trace-summary-json";

    private static final String ES_HOST = "search.local";

    private static final int ES_PORT = 9300;

    private KafkaConsumer<String, String> consumer = null;

    private TransportClient client = null;


    public KafkaESSummaryConsumer()
    {
        System.out.println("Kafka summary consumer started");

        LOG.info("Kafka summary consumer started");

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "summaryConsumerGroup");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer(props);

        LOG.info("consumer listening to kafka:29092");

        //es search

        try
        {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(ES_HOST), ES_PORT));

            LOG.info("es search client connected and {}:{}", ES_HOST, ES_PORT);
        }
        catch (UnknownHostException e)
        {
            e.printStackTrace();
        }

    }

    @Override
    public void run()
    {
        try
        {
            LOG.info("subscribing to topic: " + TOPIC_NAME);

            consumer.subscribe(Arrays.asList(TOPIC_NAME));

            while (true)
            {
                try
                {
                    Thread.sleep(500);

                    int tmpCounter = 0;

                    ConsumerRecords<String, String> records = consumer.poll(100);

                    BulkRequestBuilder bulkRequest = client.prepareBulk();

                    for (ConsumerRecord<String, String> record : records)
                    {
                        String key = record.key();

                        String value = record.value();

                        JSONObject data = new JSONObject(value);

                        List<Object> list = data.getJSONArray("serviceNames").toList();

                        StringBuilder sb = new StringBuilder();

                        list.forEach(o -> sb.append((String) o).append(" "));

                        data.put("serviceNamesStr", sb.toString());

                        long diff = (data.getInt("endTime") - data.getInt("startTime")) / 1000;

                        data.put("duration", (diff / 1000));

                        String status = "PASS";

                        JSONObject traceEventSummary = data.getJSONObject("traceEventSummary");

                        data.put("traceEventSummary", traceEventSummary);

                        if (traceEventSummary.getInt("ERROR") > 0 || traceEventSummary.getInt("CRITICAL") > 0)
                        {
                            status = "FAIL";
                        }
                        else if (diff > 4000)
                        {
                            status = "SLOW";
                        }

                        data.put("status", status);

                        LOG.info("key: {}, value: {}", key, data.toString());

                        bulkRequest.add(client.prepareIndex("summaries", "su", key)
                                .setSource(data.toString(), XContentType.JSON)).get();

                        tmpCounter++;
                    }

                    LOG.info("tmpCounter: " + tmpCounter);

                    if (tmpCounter == 0)
                    {
                        LOG.info("Consumer poll returns empty");
                    }

                    consumer.commitAsync();

                    if (tmpCounter > 0)
                    {
                        BulkResponse bulkResponse = bulkRequest.get();

                        ESUtil.logResponse(bulkResponse);
                    }

                    bulkRequest = null;

                    Thread.sleep(500);
                }
                catch (Exception e)
                {
                    LOG.error("Error occurred", e);
                }
            }
        }
        catch (WakeupException e)
        {
            LOG.error("Kafka exception occurred", e);
        }
        finally
        {
            consumer.commitSync();
            consumer.close();
        }
    }
}

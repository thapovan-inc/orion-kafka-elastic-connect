package com.thapovan.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.text.DateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaESFootPrintConsumer implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger(KafkaESFootPrintConsumer.class);

    private static final String TOPIC_NAME = "fat-trace-object";

    private static final String TOPIC_NAME_INCOMPLETE = "fat-incomplete-trace-object";

    private static final String ES_HOST = "search.local";

    private static final int ES_PORT = 9300;

    private KafkaConsumer<String, String> consumer = null;

    private TransportClient client = null;

    public KafkaESFootPrintConsumer()
    {

        LOG.info("Kafka footprint consumer started");

        // kafka

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fpConsumerGroup");

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
            LOG.error("Error connecting es serach " + e.getMessage(), e);
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
                    int tmpCounter = 0;

                    Map<String, String> incompleteTracesMap = null;

                    ConsumerRecords<String, String> records = consumer.poll(100);

                    BulkRequestBuilder bulkRequest = client.prepareBulk();

                    for (ConsumerRecord<String, String> record : records)
                    {
                        String key = record.key();

                        String value = record.value();

                        long ts = System.currentTimeMillis();

                        try
                        {
                            JSONObject fatJson = new JSONObject(value);

                            long startTime = fatJson.getLong("startTime");

                            long endTime = fatJson.getLong("endTime");

                            Map<String, Object> doc = new HashMap<>();

                            doc.put("traceId", key);

                            doc.put("startTime", startTime);

                            doc.put("endTime", endTime);

                            doc.put("traceIncomplete", false);

                            if (startTime <= 0 || endTime <= 0)
                            {
                                doc.put("traceIncomplete", true);

                                if (incompleteTracesMap == null)
                                {
                                    incompleteTracesMap = new HashMap<>();
                                }

                                incompleteTracesMap.put(key, value);
                            }

                            doc.put("life_cycle_json", value);

                            doc.put("timestamp", ts);

                            doc.put("inserted_at", DateFormat
                                    .getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM)
                                    .format(System.currentTimeMillis()));

                            LOG.info("key: {}, value: {}", key, doc);

                            bulkRequest.add(client.prepareIndex("footprint", "fp", record.key())
                                    .setSource(doc, XContentType.JSON)).get();

                            tmpCounter++;
                        }
                        catch (Exception e)
                        {
                            LOG.error("Error proccessing fat json record; " + e.getMessage() + ", traceId: " + key, e);
                        }
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

                    if (incompleteTracesMap != null && incompleteTracesMap.size() > 0)
                    {
                        produceIncompleteTraces(incompleteTracesMap);
                    }

                    bulkRequest = null;

                    Thread.sleep(5000);
                }
                catch (Exception e)
                {
                    LOG.error("Error occurred", e);
                }
            }
        }
        catch (Exception e)
        {
            LOG.error("Kafka exception occurred", e);
        }
        finally
        {
            consumer.commitSync();
            consumer.close();
        }
    }

    public void produceIncompleteTraces(Map<String, String> traces)
    {
        if (traces == null || traces.size() == 0)
        {
            LOG.info("No records available to produce");
            return;
        }

        try
        {

            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");

            props.put(ProducerConfig.CLIENT_ID_CONFIG, "com.thapovan.kafka.KafkaESFootPrintConsumer");

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10000000);

            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            traces.forEach((k, v) ->
            {
                producer.send(new ProducerRecord<>(TOPIC_NAME_INCOMPLETE, k, v));
            });

            producer.close();
        }
        catch (Exception e)
        {
            LOG.error("Error producing incomplete topic to kafka: " + e.getMessage(), e);
        }
    }
}

package com.thapovan.kafka;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Properties;

public class ProcessIncompleteRecords implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger(ProcessIncompleteRecords.class);

    private static final Gson gson = new Gson();

    private static final String TOPIC_NAME = "trace-summary-json";

    private static final String ES_HOST = "search.local";

    private static final int ES_PORT = 9300;

    private KafkaProducer<String, String> producer = null;

    private TransportClient client = null;

    public ProcessIncompleteRecords()
    {
        LOG.info("Kafka footprint consumer started");

        // kafka

        try
        {

            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");

            props.put(ProducerConfig.CLIENT_ID_CONFIG, "ProcessIncompleteRecords");

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            producer = new KafkaProducer<>(props);

        }
        catch (Exception e)
        {
            LOG.error("Error starting kafka producer - " + e.getMessage(), e);
        }

        LOG.info("producer listening to kafka:29092");

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

    }

    private void getIncompleteTraces()
    {
        SearchResponse response = client.prepareSearch("footprint")
                .setTypes("fp")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("traceIncomplete", true))
                .setQuery(QueryBuilders.termQuery("data_read", 0))
                .setSize(10)
                .setExplain(true)
                .get();

        for (SearchHit hit : response.getHits().getHits())
        {
            JSONObject recordJson = new JSONObject(hit.getSourceAsString());

            LOG.info("recordJson: {}", recordJson);


        }
    }

    public static void main(String args[])
    {
        Calendar now = Calendar.getInstance();

        System.out.println("CURRENT TIME: " + now.getTime());

        now.add(Calendar.MINUTE, -1);

        System.out.println("BEFORE 1 MIN: " + now.getTime());
    }
}

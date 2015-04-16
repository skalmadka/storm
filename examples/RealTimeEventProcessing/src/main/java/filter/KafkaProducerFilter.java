package filter;

import backtype.storm.tuple.Values;
import filter.bloomfilter.RedisBloomFilter;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.producer.async.ProducerSendThread;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by sunil on 4/15/15.
 */
public class KafkaProducerFilter   extends BaseFilter {
    kafka.javaapi.producer.Producer<String, String> producer;
    final String kafkaTopic = "crawl";

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        //TODO: Read properties from external source
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        this.producer = new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));
    }

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        String hrefList = tridentTuple.getString(0);
        Integer depth = Integer.parseInt(tridentTuple.getString(1));

        if(hrefList == null || hrefList.trim().length() == 0 || depth == 0)
            return true;//Always pass tuple downstream. However skip recursively adding this Href URL to kafka.
        depth--;

        String[] hrefArray = hrefList.split(" ");
        for(String href : hrefArray) {
            producer.send(new KeyedMessage<String, String>(kafkaTopic, href+" "+depth.toString()));
        }

        return true;
    }
}
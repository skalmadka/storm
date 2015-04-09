import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import common.EventTridentTupleMapper;
import filter.URLFilter;
import function.GetAdFreeWebPage;
import function.PrepareCrawledPageDocument;
import function.PrepareEventDocument;
import function.PrepareHrefKafka;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TridentKafkaStateFactory;
import storm.kafka.trident.TridentKafkaUpdater;
import storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.trident.selector.DefaultTopicSelector;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

public class WebCrawlerTopology {
    public static StormTopology buildTopology(LocalDRPC drpc,Settings settings) {
        TridentTopology topology = new TridentTopology();

        //Kafka Spout
        BrokerHosts zk = new ZkHosts("localhost");
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "crawl");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        //ElasticSearch Persistent State
        StateFactory esStateFactory = new ESIndexState.Factory<String>(new ClientFactory.NodeClient(settings.getAsMap()), String.class);

        //Kafka State
        TridentKafkaStateFactory kafkaStateFactory = new TridentKafkaStateFactory()
                .withKafkaTopicSelector(new DefaultTopicSelector("crawl"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("refURL", "refDepth"));


        //Topology
        Stream s = topology.newStream("crawlKafkaSpout", spout).parallelismHint(5)
                .each(new Fields("str"), new URLFilter())
                .each(new Fields("str"), new GetAdFreeWebPage(), new Fields("url", "content_html", "title", "href", "depth"));

                //To Elasticsearch
       TridentState b1 =  s.each(new Fields("url", "content_html", "title", "href", "depth"), new PrepareCrawledPageDocument(), new Fields("index", "type", "id", "source"))
                .partitionPersist(esStateFactory, new Fields("index", "type", "id", "source"), new ESIndexUpdater<String>(new EventTridentTupleMapper()), new Fields());

 /*               //To Kafka Feedback
        TridentState b2 =  s.each(new Fields("href", "depth"), new PrepareHrefKafka(), new Fields("refURL", "refDepth"))
                .partitionPersist(kafkaStateFactory, new Fields("refURL", "refDepth"), new TridentKafkaUpdater(), new Fields());
*/
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        //conf.setMaxSpoutPending(20);

        //Elasticsearch Settings
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("storm.elasticsearch.cluster.name", "elasticsearch")
                .put("storm.elasticsearch.hosts", "127.0.0.1:9300")
                .build();

        Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        if (args.length == 0) {
            //LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("eventProcessing", conf, buildTopology(null, settings));
        }
        else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null, settings));
        }
    }
}

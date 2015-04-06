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
import function.PrepareEventDocument;
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
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;

public class EventProcessingTopology {
    public static StormTopology buildTopology(LocalDRPC drpc,Settings settings) {
        TridentTopology topology = new TridentTopology();

        //Kafka Spout
        BrokerHosts zk = new ZkHosts("localhost");
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "test");
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        //ElasticSearch Persistent State
        StateFactory stateFactory = new ESIndexState.Factory<String>(new ClientFactory.NodeClient(settings.getAsMap()), String.class);

        //Topology
        topology.newStream("eventKafkaSpout", spout).parallelismHint(5)
                .each(new Fields("str"), new PrepareEventDocument(), new Fields("index", "type", "id", "source"))
                .partitionPersist(stateFactory, new Fields("index", "type", "id", "source"), new ESIndexUpdater<String>(new EventTridentTupleMapper()));
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

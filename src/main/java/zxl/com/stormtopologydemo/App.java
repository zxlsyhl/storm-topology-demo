package zxl.com.stormtopologydemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class App {
    @Test
    public  void testTopoloy() throws Exception {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("word",new WordSpout(),1);
        topologyBuilder.setBolt("receive",new RecieveBolt(),1).shuffleGrouping("word");
        topologyBuilder.setBolt("print",new ConsumeBolt(),1).shuffleGrouping("receive");

        // 集群运行
//        Config config = new Config();
//        config.setNumWorkers(3);
//        config.setDebug(true);
//        StormSubmitter.submitTopology("teststorm", config, topologyBuilder.createTopology());

        // 本地测试
         Config config = new Config();
         config.setNumWorkers(3);
         config.setDebug(true);
         config.setMaxTaskParallelism(20);
         LocalCluster cluster = new LocalCluster();
         cluster.submitTopology("wordCountTopology", config, topologyBuilder.createTopology());
         Utils.sleep(600000);
         // 执行完毕，关闭cluster
         cluster.shutdown();
    }

    @Test
    public  void testKakfaSpoutTopoloy() throws Exception {
        final TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafka_spout", new KafkaSpout<String,String>(newKafkaSpoutConfig("nginxlog")));
        topologyBuilder.setBolt("receive",new RecieveKafkaBolt(),1).shuffleGrouping("kafka_spout", "stream1");
        topologyBuilder.setBolt("print",new ConsumeBolt(),1).shuffleGrouping("receive");

        // 集群运行
//        Config config = new Config();
//        config.setNumWorkers(3);
//        config.setDebug(true);
//        StormSubmitter.submitTopology("teststorm", config, topologyBuilder.createTopology());

        // 本地测试
        Config config = new Config();
        config.setNumWorkers(1);
        config.setDebug(true);
//        config.setMaxTaskParallelism(20);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCountTopology", config, topologyBuilder.createTopology());
        Utils.sleep(600000);
        // 执行完毕，关闭cluster
        cluster.shutdown();
    }

    private static KafkaSpoutConfig<String,String> newKafkaSpoutConfig(String topic) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                new Fields("topic", "partition", "offset", "key", "value"), "stream1");
        //bootstrapServer 以及topic
        return KafkaSpoutConfig.builder("192.168.1.105:9092", topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup_1")
                .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 200)
                .setRecordTranslator(trans)
                .setRetry(getRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    @Test
    public  void testKakfaSpoutTopoloy2() throws Exception {
        KafkaSpoutConfig.Builder<String,String> kafkaBuild = KafkaSpoutConfig.builder("192.168.1.105:9092","nginxlog");
        kafkaBuild.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);
        kafkaBuild.setOffsetCommitPeriodMs(100);//设置多长时间向kafka提交一次offset
        kafkaBuild.setProp(ConsumerConfig.GROUP_ID_CONFIG,"testGroup");
        kafkaBuild.setProp(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,1);
        kafkaBuild.setProp(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,0);
        KafkaSpoutConfig<String,String> build = kafkaBuild.build();
        KafkaSpout<String,String> kafkaSpout = new KafkaSpout<>(build);
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafka_spout",kafkaSpout,1);
        topologyBuilder.setBolt("receive",new RecieveKafkaBolt(),1).shuffleGrouping("kafka_spout");
        topologyBuilder.setBolt("print",new ConsumeBolt(),1).shuffleGrouping("receive");

        // 集群运行
//        Config config = new Config();
//        config.setNumWorkers(3);
//        config.setDebug(true);
//        StormSubmitter.submitTopology("teststorm", config, topologyBuilder.createTopology());

        // 本地测试
        Config config = new Config();
        config.setNumWorkers(1);
        config.setDebug(true);
//        config.setMaxTaskParallelism(20);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCountTopology", config, topologyBuilder.createTopology());
        Utils.sleep(600000);
        // 执行完毕，关闭cluster
        cluster.shutdown();
    }
}

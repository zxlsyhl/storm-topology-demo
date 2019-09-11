package zxl.com.stormtopologydemo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class ConsumeBolt extends BaseRichBolt {
    private static final long serialVersionUID = -7114915627898482737L;

    private FileWriter fileWriter = null;

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;

        try {
            // TODO: 2019/9/9 要随着本地和集群上变动
//            fileWriter = new FileWriter("/usr/local/tmpdata/" + UUID.randomUUID());
             fileWriter = new FileWriter("E:\\workspace\\storm-topology-demo\\test\\" + UUID.randomUUID());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void execute(Tuple tuple) {

        try {
            String word = tuple.getStringByField("word") + "......." + "\n";
            fileWriter.write(word);
            fileWriter.flush();
            System.out.println(word);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

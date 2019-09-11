package zxl.com.stormtopologydemo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class WordSpout extends BaseRichSpout {
    private static final long serialVersionUID = 6102239192526611945L;

    private SpoutOutputCollector collector;

    Random random = new Random();


    // 初始化tuple的collector
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        // 模拟产生消息队列
        String[] words = {"iphone","xiaomi","mate","sony","sumsung","moto","meizu"};

        final String word = words[random.nextInt(words.length)];

        // 提交一个tuple给默认的输出流
        this.collector.emit(new Values(word));

        Utils.sleep(5000);
    }

    // 声明发送消息的字段名
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

}

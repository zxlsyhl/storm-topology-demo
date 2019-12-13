package zxl.com.stormtopologydemo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class RecieveBolt extends BaseRichBolt {
    private static final long serialVersionUID = -4758047349803579486L;

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        // 将spout传递过来的tuple值进行转换
        this.collector.emit(new Values(tuple.getStringByField("word") + "!!!"));
//        collector.ack(tuple);
//        collector.fail(tuple);
    }

    // 声明发送消息的字段名
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}

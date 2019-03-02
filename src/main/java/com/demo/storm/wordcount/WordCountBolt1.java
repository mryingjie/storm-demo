package com.demo.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author ZhengYingjie
 * @time 2019/3/2 17:14
 * @description
 */
public class WordCountBolt1 extends BaseRichBolt {

    OutputCollector collector;

    /**
     * 初始化方法
     * @param stormConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 被storm循环调用 接受上游的Tuple 处理发送给下游
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        // 0 是下标
        String string = input.getString(0);
        //通过上游声明的字段取得对应的values下标
//        input.getStringByField("aaa");

        String[] split = string.split(" ");

        for (String s : split) {
            collector.emit(new Values(s,1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","num"));
    }
}

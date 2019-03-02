package com.demo.storm.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author ZhengYingjie
 * @time 2019/3/2 17:03
 * @deption
 */
public class WordCountSpout extends BaseRichSpout {

    SpoutOutputCollector collector;

    /**
     * 初始化方法
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    /**
     * storm循环调用获取数据源的数据
     */
    @Override
    public void nextTuple() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        collector.emit(new Values("i am lilei love hanmeimei","aaa Value"));
    }

    /**
     * 给发射出去的数据Values的每个下标起个名字
     * 在这里即：
     * love => i am lilei love hanmeimei
     * aaa => aaa Value
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("love","aaa"));
    }
}

package com.demo.storm.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author ZhengYingjie
 * @time 2019/3/2 17:03
 * @deption
 */
public class WordCountSpout extends BaseRichSpout {

    SpoutOutputCollector collector;

    Map tupleBuffer = new HashMap();

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
        String msgId = UUID.randomUUID().toString().replaceAll("-", "");
        Values tuple = new Values("i am lilei love hanmeimei", "aaa Value");
        collector.emit(tuple,msgId);
        tupleBuffer.put(msgId,tuple );
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

    @Override
    public void ack(Object msgId) {
        System.out.println("消息处理成功msgId="+msgId);
        tupleBuffer.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("消息处理失败msgId="+msgId);
        Values tuple = (Values) tupleBuffer.get(msgId);
        collector.emit(tuple,msgId);
    }
}

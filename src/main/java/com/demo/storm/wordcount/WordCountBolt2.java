package com.demo.storm.wordcount;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;



import java.util.HashMap;
import java.util.Map;

/**
 * @author ZhengYingjie
 * @time 2019/3/2 17:25
 * @description
 */
public class WordCountBolt2 extends BaseRichBolt {

    OutputCollector collector;

    Map<String,Integer> map;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        map = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        try {
            //getString(0)
            String word = input.getStringByField("word");

            //getString(1)
            Integer num = input.getIntegerByField("num");

            if(!map.containsKey(word)){
                map.put(word,1 );
            }else{
                map.put(word, map.get(word)+num);
            }
            collector.ack(input);

        } catch (Exception e) {
            collector.fail(input);
        }


        System.out.println(JSONObject.toJSONString(map));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

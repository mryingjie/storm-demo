package com.demo.storm.wordcount;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author ZhengYingjie
 * @time 2019/3/2 16:33
 * @description
 */
public class WordCountTopllogMain {

    public static void main(String[] args) throws AuthorizationException {

        //准备一个TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("wordCountSpout",new WordCountSpout(),1 );

        builder.setBolt("wordCoundBolt1",new WordCountBolt1(),10).shuffleGrouping("wordCountSpout");

        builder.setBolt("wordCoundBolt2",new WordCountBolt2(),2).fieldsGrouping("wordCoundBolt1",new Fields("word"));


        //创建一个configuration 指定当前topology需要的worker的数量

//        2、如何使用Ack机制
//        spout 在发送数据的时候带上msgid
//
//        设置acker数至少大于0；Config.setNumAckers(conf, ackerParal);
//        在bolt中完成处理tuple时，执行OutputCollector.ack(tuple), 当失败处理时，执行OutputCollector.fail(tuple);
//        推荐使用IBasicBolt， 因为IBasicBolt 自动封装了OutputCollector.ack(tuple), 处理失败时，请抛出FailedException，则自动执行OutputCollector.fail(tuple)

        Config config = new Config();
        Config.setNumAckers(config, 1);
//        config.setDebug(true);
        config.setNumWorkers(2);

        //提交任务
        if(args!=null&&args.length>0){
            //集群提交
            try {
                StormSubmitter.submitTopology("wordCpunt", config,builder.createTopology() );
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }

        }else{
            //本地提交
            LocalCluster localCluster = new LocalCluster();

            localCluster.submitTopology("wordCount", config, builder.createTopology());
        }

    }

}

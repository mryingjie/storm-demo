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

    public static void main(String[] args) {

        //准备一个TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("wordCountSpout",new WordCountSpout(),1 );

        builder.setBolt("wordCoundBolt1",new WordCountBolt1(),10).shuffleGrouping("wordCountSpout");

        builder.setBolt("wordCoundBolt2",new WordCountBolt2(),1).noneGrouping("wordCoundBolt1");


        //创建一个configuration 指定当前topology需要的worker的数量

        Config config = new Config();
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
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }

        }else{
            //本地提交
            LocalCluster localCluster = new LocalCluster();

            localCluster.submitTopology("wordCount", config, builder.createTopology());
        }

    }

}

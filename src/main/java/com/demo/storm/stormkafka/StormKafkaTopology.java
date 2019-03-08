package com.demo.storm.stormkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * @author ZhengYingjie
 * @time 2019/3/8 9:26
 * @description
 * 本实例详细说明storm和kafka的结合，
 * 具体流程是：生产者着将数据写入到kafka的topic1中，
 * storm应用做为消费者，从topic1中读取数据，并做一些复杂的处理变换
 * ，然后将转换后的数据写入kafka的topic2中。
 */
public class StormKafkaTopology {

    public static void main(String[] args) throws AuthorizationException {
        //准备一个TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();

        //该类将传入的kafka记录转换为storm的tuple
        ByTopicRecordTranslator<String,String> brt =
                new ByTopicRecordTranslator<>( (consumerRecord) -> new Values(consumerRecord.value(),consumerRecord.topic()),new Fields
                        ("values","topic"));

//        以上代码的完整体
//        ByTopicRecordTranslator<String,String> brt2 = new ByTopicRecordTranslator<>(new Func<ConsumerRecord<String, String>, List<Object>>() {
//            @Override
//            public List<Object> apply(ConsumerRecord<String, String> consumerRecord) {
//                return new Values(consumerRecord.value(),consumerRecord.topic());
//            }
//        },new Fields("values","test7"));

        //设置要消费的topic即test7  此处返回的对象其实就是上一步构建的brt  即方法内部return this
        brt = brt.forTopic("payment", (consumerRecord) -> new Values(consumerRecord.value(), consumerRecord.topic()),
                new Fields("values", "topic"));

        //类似之前的SpoutConfig
        KafkaSpoutConfig<String,String> ksc = KafkaSpoutConfig
                //bootstrapServers 以及topic(test7)
                .builder("192.168.42.132:9092", "payment")
                //设置group.id
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "skc-test")
                //设置开始消费的起始位置 kafka spout轮询记录的偏移量大于分区中的最后一个偏移量
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST)
                //设置提交消费边界的时长间隔 等价于10000
                .setOffsetCommitPeriodMs(10_000)
                //设置将传入的kafka记录转换为storm的tuple的转换器 Translator
                .setRecordTranslator(brt)
                .build();

        //设置Spout 和 Bolt 两个并发度意味着两个topic的分区
        builder.setSpout("kafkaspout", new KafkaSpout<>(ksc), 2);
        builder.setBolt("mybolt1", new kafkaBolt(), 4).fieldsGrouping("kafkaspout",new Fields("topic"));


        Config config = new Config();
        config.setNumWorkers(2);
        config.setNumAckers(0);
        //提交任务
        if(args!=null&&args.length>0){
            //集群提交
            try {
                StormSubmitter.submitTopology("kafkaStorm", config,builder.createTopology() );
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }

        }else{
            //本地提交
            LocalCluster localCluster = new LocalCluster();

            localCluster.submitTopology("StormKafkaTopology", config, builder.createTopology());
        }
    }


}

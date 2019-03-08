package com.demo.storm.stormkafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author ZhengYingjie
 * @time 2019/3/8 11:29
 * @description
 */
public class kafkaBolt extends BaseRichBolt {

    OutputCollector outputCollector = null;


    private static final String BROKER_LIST = "192.168.42.132:9092";
    private static KafkaProducer<String, String> producer = null;

    /**
     * 初始化生产者
     */
    static {
        Properties configs = initConfig();
        producer = new KafkaProducer<String, String>(configs);
    }

    /**
     * 初始化配置
     * @return
     */
    private static Properties initConfig() {
        /**
         * 1.指定当前kafka producer生产者发送数据的目的地
         * 创建topic的命令：
         * ./kafka-topics.sh --create --topic order --replication-factor 2 --partitions 4 --zookeeper hadoop1002:2181
         */

        /**
         * 2.读取配置文件
         */
        Properties props = new Properties();

        /**
         * key.serializer.class 默认为Serializable.class
         */
//        props.put("serializer.class", "kafka.serializer.StringDecoder");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        /**
         * kafak broker对应的主机
         */
//        props.put("metadate.broker.list", "hadoop.abc6.net:9092,zhengyingjie2.abc6.net:9092,zhengyingjie3.abc6.net:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

        /**
         * request.required.acks 设置发送数据是否需要服务器的反馈 三个值0 1 -1
         * 0 表示producer永远不会等待一个来自broker的ack
         * 1 表示在leader replica收到数据后 就会返回ack
         * 但是如果刚写到leader 还没写到replica上就挂掉了 数据可能丢失
         * -1 表示所有的副本都收到数据了才会返回ack
         *
         * 默认
         */
        props.put(ProducerConfig.ACKS_CONFIG, "1");


        /**
         * 分区配置 默认是org.apache.kafka.clients.producer.internals.DefaultPartitioner
         * 可以自定义
         */
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());

        return props;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        System.out.println("topologyContext:"+JSON.toString(topologyContext));
        System.out.println("stormConf:"+JSON.toString(stormConf));
    }

    /**
     * 被storm循环调用 接受上游的Tuple 处理发送给下游
     * @param tuple
     */

    @Override
    public void execute(Tuple tuple) {
        String topic = tuple.getStringByField("topic");
        String values = tuple.getStringByField("values");
        System.out.println("excute:\n"+"topic="+topic+"\nvalues="+values);
        String newTopic = topic+"--new";
        String newvalues = values+"--new";
        String key = UUID.randomUUID().toString();
        ProducerRecord<String,String> producerRecord =
                new ProducerRecord<>(newTopic,null,null,key,newvalues,null);
        producer.send(producerRecord, (metadata, exception) -> {
            System.out.println("执行Callback！！！");

            System.out.println("metadata:{"+"partition:"+metadata.partition()+" topic:"+metadata.topic()+" " +
                    "offset:"+metadata.offset()+"}");
        });

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

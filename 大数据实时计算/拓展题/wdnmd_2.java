import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.*;
//import org.apache.flink.streaming.api.scala._;


public class wdnmd_2 {
    //  Kafka主题名称
    public static final String inputTopic = "mn_buy_ticket_1_zhl";

    //  用于存储到达城市的哈希表
    public static HashMap<String, Integer> cityCounts = new HashMap<>();

    //  用于计数已经读入的数据数量
    public static int readingCount = 0;

    //  Kafka地址
    public static final String bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037";

    public static void main(String[] args) throws Exception {
        //  获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  设置属性
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", bootstrapServers);
        kafkaProperties.put("group.id", UUID.randomUUID().toString());
        kafkaProperties.put("auto.offset.reset", "earliest");
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<String>(inputTopic, new SimpleStringSchema(), kafkaProperties);
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> inputKafkaStream = env.addSource(kafkaConsumer);
        //        inputKafkaStream.print().setParallelism(1);

        inputKafkaStream.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                ++readingCount;
                String[] splits = s.split(",");
                for (String str: splits) {
                    if (str.contains("destination")) {
                        String city = str.split(":")[1];
                        if (cityCounts.containsKey(city)) {
                            int cnt = cityCounts.get(city);
                            cityCounts.put(city, cnt + 1);
                        } else {
                            cityCounts.put(city, 1);
                        }
                    }
                }

                if (readingCount == 44191) {
//                    System.out.println(readingCount);
                    for (String key: cityCounts.keySet()) {
                        System.out.println("{" + key + ": " +
                                cityCounts.get(key) + "}");
                    }

                    //  只有list才能排序，哈希表是无序的
                    ArrayList<Map.Entry<String, Integer>> list =
                            new ArrayList<>(cityCounts.entrySet());
                    list.sort(new Comparator<Map.Entry<String, Integer>>() {
                        @Override
                        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                            //  变换一下o1、o2的顺序就能决定是从小到大还是从大到小排序
                            return o2.getValue().compareTo(o1.getValue());
                        }
                    });
                    int i = 0;
                    for (Map.Entry<String, Integer> e: list) {
                        if (i >= 5)
                            break;
                        else
                            ++i;
                        System.out.println(e.getKey() + ": " + e.getValue());
                    }
                }

                return null;
            }
        });

        env.execute("Streaming From Kafka");
    }
}

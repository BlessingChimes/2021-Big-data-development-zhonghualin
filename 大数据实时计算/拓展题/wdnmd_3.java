import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class wdnmd_3 {
    //  数据库连接
    private static Connection conn;
    private static Statement stmt;

    //  数据库连接参数
    private static final String url =
            "jdbc:hive2://bigdata129.depts.bingosoft.net:22129/user36_db";
    private static final String user = "user36";
    private static final String password = "pass@bingo36";
    private static final String driver = "org.apache.hive.jdbc.HiveDriver";

    //  kafka参数
    public static String topic = "mn_buy_ticket_1_zhl_mysql";
    public static String bootstrapServers = "bigdata35.depts.bingosoft.net:29035," +
                                            "bigdata36.depts.bingosoft.net:29036," +
                                            "bigdata37.depts.bingosoft.net:29037";

    public static void main(String[] args) {
        ArrayList<String> data = readMySQL();
        produceMySQLToKafka(data);
    }

    public static ArrayList<String> readMySQL(){
        ArrayList<String> data = null;
        //  加载驱动类
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            String sql = "select * from nmsl";
            data = new ArrayList<>();
            ResultSet rs = stmt.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();
//            StringBuilder columnNames = new StringBuilder();
//            for (int i = 0; i < rsmd.getColumnCount(); ++i) {
//                columnNames.append(rsmd.getColumnLabel(i + 1));
//                if (i != rsmd.getColumnCount() - 1) {
//                    columnNames.append(",");
//                }
//            }
//            data.add(columnNames.toString());

            while (rs.next()) {
                StringBuilder rowData = new StringBuilder();
                for (int i = 0; i < rsmd.getColumnCount(); ++i) {
                    rowData.append(rs.getString(i + 1));
                    if (i != rsmd.getColumnCount() - 1) {
                        rowData.append(",");
                    }
                }
                data.add(rowData.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return data;
    }

    public static void produceMySQLToKafka(ArrayList<String> data) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common." +
                "serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common." +
                "serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (String s: data) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, s);
            System.out.println("开始生产数据：" + s);
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }
}

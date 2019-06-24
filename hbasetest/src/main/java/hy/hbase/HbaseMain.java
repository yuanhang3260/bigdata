package hy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseMain {

  // Config
  private static Configuration conf = null;
  static {
    conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");
  }

  public static void main(String[] args) throws Exception {
    testPut();
  }

  private static void testPut() throws Exception {
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("table_dog"));

    Put put = new Put(Bytes.toBytes("rk001"));
    put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("name"), Bytes.toBytes("snoopy"));
    put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("age"), Bytes.toBytes("3"));
    put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("sex"), Bytes.toBytes("boy"));

    table.put(put);
    table.close();
  }

}
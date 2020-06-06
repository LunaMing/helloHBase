package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class HelloHBaseTest {
    HelloHBase helloHBase = new HelloHBase();
    Connection connection;

    @Before
    public void setUp() throws Exception {
        // 新建一个Configuration
        Configuration conf = HBaseConfiguration.create();
        // 集群的连接地址，在控制台页面的数据库连接界面获得(注意公网地址和VPC内网地址)
        conf.set("hbase.zookeeper.quorum", "hb-proxy-pub-bp15ttv4g8k160271-001.hbase.rds.aliyuncs.com:2181");
        // 设置用户名密码，默认root:root，可根据实际情况调整
        conf.set("hbase.client.username", "root");
        conf.set("hbase.client.password", "root");
        // 如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        //conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());

        System.out.println("开始连接……");

        // 创建 HBase连接，在程序生命周期内只需创建一次，该连接线程安全，可以共享给所有线程使用。
        // 在程序结束后，需要将Connection对象关闭，否则会造成连接泄露。
        // 也可以采用try finally方式防止泄露
        connection = ConnectionFactory.createConnection(conf);

        System.out.println("连接成功，connection：" + connection.toString());

/*        //DDL
        Admin admin = connection.getAdmin();

        // 建表
//                System.out.println("开始创建table...");
//                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("tablename"));
//                htd.addFamily(new HColumnDescriptor(Bytes.toBytes("family")));
//                // 创建一个只有一个分区的表
//                // 在生产上建表时建议根据数据特点预先分区
//                admin.createTable(htd);
//                System.out.println("table创建成功！");

//                // disable 表
//                admin.disableTable(TableName.valueOf("tablename"));

//                // truncate 表
//                admin.truncateTable(TableName.valueOf("tablename"), true);

        // 删除表
//                System.out.println("开始删除table...");
//                admin.deleteTable(TableName.valueOf("tablename"));
//                System.out.println("table删除成功！");*/

/*        //DML
        //Table 为非线程安全对象，每个线程在对Table操作时，都必须从Connection中获取相应的Table对象
        Table table = connection.getTable(TableName.valueOf("tablename"));
        // 插入数据
        System.out.println("开始插入数据...");
        Put put = new Put(Bytes.toBytes("row"));
        put.addColumn(Bytes.toBytes("family"), Bytes.toBytes("abc"), Bytes.toBytes("value"));
        table.put(put);
        System.out.println("插入数据成功！");

        // 单行读取
        System.out.println("开始读取单行...");
        Get get = new Get(Bytes.toBytes("row"));
        Result res = table.get(get);
        System.out.println("单行读取结果为：" + res.toString());

        // 删除一行数据
                System.out.println("开始删除row...");
                Delete delete = new Delete(Bytes.toBytes("row"));
                table.delete(delete);
                System.out.println("row删除成功！");

        // 插入数据
        System.out.println("开始插入数据...");
        Put put2 = new Put(Bytes.toBytes(1));
        put2.addColumn(Bytes.toBytes("family"), Bytes.toBytes("abc2"), Bytes.toBytes("1"));
        table.put(put2);
        System.out.println("插入数据成功！");

        // scan 范围数据
        System.out.println("开始扫描范围数据...");
//                Scan scan = new Scan(Bytes.toBytes("startRow"), Bytes.toBytes("endRow"));
        Scan scan = new Scan(Bytes.toBytes(1), Bytes.toBytes(1));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            // 处理查询结果result
            System.out.println("toString: " + result.toString());
        }
        scanner.close();*/
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void createTable() throws IOException {
        //准备
        String tableName = "Student";
        String[] fields = {"S_No", "S_Name", "S_Sex", "S_Age"};
        //创建
        helloHBase.createTable(tableName, fields);
        //验证
        Table table = connection.getTable(TableName.valueOf(tableName));
        String rowName = "0";
        byte[] rowByte = rowName.getBytes();
        String familyName = "S_No";
        byte[] familyByte = familyName.getBytes();
        String qualifier = "qualifier";
        byte[] qualiByte = qualifier.getBytes();
        String value = "value";
        byte[] valueByte = value.getBytes();
        //插入
        Put put = new Put(rowByte);
        put.addColumn(familyByte, qualiByte, valueByte);
        table.put(put);
        //获取
        Result res = table.get(new Get(rowByte));
        byte[] resValue = res.getValue(familyByte, qualiByte);
        //比对结果
        Assert.assertArrayEquals(valueByte, resValue);
        //删掉测试数据
        Delete delete = new Delete(rowByte);
        table.delete(delete);
    }

    @Test
    public void addRecord() {
    }

    @Test
    public void scanColumn() {
    }

    @Test
    public void modifyData() {
    }

    @Test
    public void deleteRow() {
    }
}
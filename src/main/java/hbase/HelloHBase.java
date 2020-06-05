package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HelloHBase {
    public static void main(String[] args) {
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
        try (Connection connection = ConnectionFactory.createConnection(conf)) {

            System.out.println("连接成功，connection：" + connection.toString());

            //DDL
            try (Admin admin = connection.getAdmin()) {

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
//                System.out.println("table删除成功！");
            } catch (IOException e) {
                e.printStackTrace();
            }

            //DML
            //Table 为非线程安全对象，每个线程在对Table操作时，都必须从Connection中获取相应的Table对象
            try (Table table = connection.getTable(TableName.valueOf("tablename"))) {
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
//                System.out.println("开始删除row...");
//                Delete delete = new Delete(Bytes.toBytes("row"));
//                table.delete(delete);
//                System.out.println("row删除成功！");

                // 插入数据
                System.out.println("开始插入数据...");
                Put put1 = new Put(Bytes.toBytes(0));
                put1.addColumn(Bytes.toBytes("family"), Bytes.toBytes("abc1"), Bytes.toBytes("0"));
                table.put(put1);
                System.out.println("插入数据成功！");

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
                scanner.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * 要求当 HBase 已经存在名为 tableName 的表的时候，先删除原有的表，然后再创建新的表。
     *
     * @param tableName 表的名称
     * @param fields    存储记录各个域名称的数组
     */
    public void createTable(String tableName, String[] fields) {

    }

    /**
     * 添加数据
     * 其中 fields 中每个元素如果对应的列族下还有相应的列限定符的话，用“columnFamily:column”表示。
     * 例如：
     * 同时向“Math”、“Computer Science”、“English”三列添加成绩时，
     * 字符串数组 fields 为{“Score:Math”, ” Score:Computer Science”, ”Score:English” }，
     * 数组 values 存储这三门课的成绩。
     *
     * @param tableName 表名
     * @param row       行（用 S_Name 表示）
     * @param fields    指定的单元格
     * @param values    数据
     */
    public void addRecord(String tableName, String row, String[] fields, String[] values) {

    }

    /**
     * 查找数据
     * 浏览表 tableName 某一列的数据，如果某一行记录中该列数据不存在，则返回 null。
     * 要求当参数 column 为某一列族名称时，如果底下有若干个列限定符，则要列出每个列限定符代表的列的数据；
     * 当参数 column 为某一列具体名称（例如“Score:Math”）时，只需要列出该列的数据。
     *
     * @param tableName 表名
     * @param column    列名 或 列族名
     */
    public void scanColumn(String tableName, String column) {

    }

    /**
     * 修改数据
     * 修改表 tableName，行 row（可以用学生姓名 S_Name 表示），列 column 指定的单元格的数据。
     *
     * @param tableName 表名
     * @param row       行（用学生姓名 S_Name 表示）
     * @param column    列
     */
    public void modifyData(String tableName, String row, String column) {

    }

    /**
     * 删除表 tableName 中 row 指定的行的记录。
     *
     * @param tableName 表名
     * @param row       行（用学生姓名 S_Name 表示）
     */
    public void deleteRow(String tableName, String row) {

    }
}
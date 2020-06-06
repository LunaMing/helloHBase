package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HelloHBase {

    // 新建一个Configuration
    private static final Configuration conf = HBaseConfiguration.create();


    //配置连接参数conf，包括连接地址、用户名、密码
    static {
        // 集群的连接地址，在控制台页面的数据库连接界面获得(注意公网地址和VPC内网地址)
        conf.set("hbase.zookeeper.quorum", "hb-proxy-pub-bp15ttv4g8k160271-001.hbase.rds.aliyuncs.com:2181");
        // 设置用户名密码，默认root:root，可根据实际情况调整
        conf.set("hbase.client.username", "root");
        conf.set("hbase.client.password", "root");
        // 如果您直接依赖了阿里云hbase客户端，则无需配置connection.impl参数，如果您依赖了alihbase-connector，则需要配置此参数
        //conf.set("hbase.client.connection.impl", AliHBaseUEClusterConnection.class.getName());
    }

    public static void main(String[] args) {
    }

    /**
     * 创建表
     * 要求当 HBase 已经存在名为 tableName 的表的时候，先删除原有的表，然后再创建新的表。
     *
     * @param tableName 表的名称
     * @param fields    存储记录各个列族名称的数组，这里就是cf的名字组
     */
    public void createTable(String tableName, String[] fields) {
        System.out.println("开始创建表：" + tableName);
        //连接
        Connection connection = null;
        // 建立连接
        try {
            // 创建 HBase连接，在程序生命周期内只需创建一次，该连接线程安全，可以共享给所有线程使用。
            // 在程序结束后，需要将Connection对象关闭，否则会造成连接泄露。
            // 也可以采用try finally方式防止泄露
            connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            //判断存在
            if (admin.tableExists(TableName.valueOf(tableName))) {
                //如果已经存在
                //就删除这个表
                //先停止表状态
                admin.disableTable(TableName.valueOf(tableName));
                //然后删除表
                admin.deleteTable(TableName.valueOf(tableName));
            }
            //现在表不存在
            //创建这个表
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建cf
            for (String f : fields) {
                hTableDescriptor.addFamily(new HColumnDescriptor(f));
            }
            // 创建一个分区的表
            admin.createTable(hTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                //关闭连接
                if (connection != null) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("table创建成功！");
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
     * @param fields    指定的单元格（每个字符串格式是“columnFamily:column”）
     * @param values    数据
     */
    public void addRecord(String tableName, String row, String[] fields, String[] values) {
        System.out.println("在表" + tableName + "开始插入数据");
        //连接
        Connection connection = null;
        // 建立连接
        try {
            // 创建 HBase连接，在程序生命周期内只需创建一次，该连接线程安全，可以共享给所有线程使用。
            // 在程序结束后，需要将Connection对象关闭，否则会造成连接泄露。
            // 也可以采用try finally方式防止泄露
            connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            byte[] rowByte = row.getBytes();
            //插入过程
            for (int i = 0; i < fields.length; i++) {
                //分割fields
                String field = fields[i];
                String[] cfAndC = field.split(":");
                String cf = cfAndC[0];
                String column = cfAndC[1];
                //数据内容
                String value = values[i];
                System.out.println("数据列：" + row + ":" + field + ":" + value);
                //建立字节流用于插入
                byte[] familyByte = cf.getBytes();
                byte[] qualiByte = column.getBytes();
                byte[] valueByte = value.getBytes();
                //插入
                Put put = new Put(rowByte);
                put.addColumn(familyByte, qualiByte, valueByte);
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                //关闭连接
                if (connection != null) {
                    connection.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("插入数据成功！");
    }

    /**
     * 查找数据
     * 浏览表 tableName 某一列的数据，如果某一行记录中该列数据不存在，则返回 null。
     * 要求当参数 column 为某一列族名称时，如果底下有若干个列限定符，则要列出每个列限定符代表的列的数据；
     * 当参数 column 为某一列具体名称（例如“Score:Math”）时，只需要列出该列的数据。
     *
     * @param tableName 表名
     * @param column    列名（格式为"Score:Math"） 或 列族名（"Score"）
     * @return 数据内容（如果是查看列，格式为"80"；如果是查看列族，格式为"Math:80, English:90"）
     */
    public String scanColumn(String tableName, String column) {
        String res = "";
        return res;
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
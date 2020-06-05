package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HelloHBase {
    public static void main(String[] args) {
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest {

	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "zoo1,zoo2,zoo3");
		configuration.set("hbase.master", "master:600000");
	}

	public static void main(String[] args) {
		String tableName = "test2";
		createTable(tableName);
		insertData(tableName);
		queryAll(tableName);
		queryByCondition1(tableName);
		queryByCondition2(tableName);
		queryByCondition3(tableName);
		deleteRow(tableName, "abc");
		deleteByCondition(tableName, "abcd");
	}

	public static void createTable(String tableName) {
		System.out.println("start create table ......");
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			Admin hBaseAdmin = connection.getAdmin();
			TableName tableName1 = TableName.valueOf(tableName);
			if (hBaseAdmin.tableExists(tableName1)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName1);
				hBaseAdmin.deleteTable(tableName1);
				System.out.println(tableName + " is exist,detele....");
			}

			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName1);
			tableDescriptor.addFamily(new HColumnDescriptor("column1"));
			tableDescriptor.addFamily(new HColumnDescriptor("column2"));
			tableDescriptor.addFamily(new HColumnDescriptor("column3"));
			hBaseAdmin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......");
	}

	public static void insertData(String tableName) {
		try {
			System.out.println("start insert data ......");
			Connection connection = ConnectionFactory.createConnection(configuration);
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);

			Put put = new Put("112233bbbcccc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
			put.addColumn("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列
			put.addColumn("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列
			put.addColumn("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列

			table.put(put);
			Put put1 = new Put("abc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
			put1.addColumn("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列
			put1.addColumn("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列
			put1.addColumn("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列

			table.put(put1);

			Put put2 = new Put("abcd".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
			put2.addColumn("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列
			put2.addColumn("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列
			put2.addColumn("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列

			table.put(put2);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end insert data ......");
	}

	public static void dropTable(String tableName) {
		try {

			Connection connection = ConnectionFactory.createConnection(configuration);
			Admin admin = connection.getAdmin();
			TableName tableName1 = TableName.valueOf(tableName);
			admin.disableTable(tableName1);
			admin.deleteTable(tableName1);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteRow(String tableName, String rowkey) {
		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);
			List<Delete> list = new ArrayList<Delete>();
			Delete d1 = new Delete(rowkey.getBytes());
			list.add(d1);
			table.delete(list);
			System.out.println("删除行成功!");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void deleteByCondition(String tablename, String rowkey) {
		// 目前还没有发现有效的API能够实现根据非rowkey的条件删除这个功能能，还有清空表全部数据的API操作

	}

	public static void queryAll(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				Cell[] cells = r.rawCells();
				for (Cell cell : cells) {
					System.out.println("列：" + new String(CellUtil.cloneFamily(cell)) + "====值:"
							+ new String(cell.getValueArray()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void queryByCondition1(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);
			Get scan = new Get("abc".getBytes());// 根据rowkey查询
			Result r = table.get(scan);
			if (!r.isEmpty()) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				Cell[] cells = r.rawCells();
				for (Cell cell : cells) {
					System.out.println("列：" + new String(CellUtil.cloneFamily(cell)) + "====值:"
							+ new String(cell.getValueArray()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void queryByCondition2(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa")); // 当列column1的值为aaa时进行查询
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				Cell[] cells = r.rawCells();
				for (Cell cell : cells) {
					System.out.println("列：" + new String(CellUtil.cloneFamily(cell)) + "====值:"
							+ new String(cell.getValueArray()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void queryByCondition3(String tableName) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			TableName tableName1 = TableName.valueOf(tableName);
			Table table = connection.getTable(tableName1);

			List<Filter> filters = new ArrayList<Filter>();

			Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa"));
			filters.add(filter1);

			Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("column2"), null, CompareOp.EQUAL,
					Bytes.toBytes("bbb"));
			filters.add(filter2);

			Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
					Bytes.toBytes("ccc"));
			filters.add(filter3);

			FilterList filterList1 = new FilterList(filters);

			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("获得到rowkey:" + new String(r.getRow()));
				Cell[] cells = r.rawCells();
				for (Cell cell : cells) {
					System.out.println("列：" + new String(CellUtil.cloneFamily(cell)) + "====值:"
							+ new String(cell.getValueArray()));
				}
			}
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
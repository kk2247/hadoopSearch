package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import util.FileContent;
import util.FileSpliter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestOper {

	public static void main(String[] args) throws UnsupportedEncodingException {

		System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.6");

		//创建配置对象
		Configuration config = HBaseConfiguration.create();
		//配置zookeeper集群
        config.set("hbase.zookeeper.quorum", "172.17.11.250,172.17.11.251,172.17.11.252");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "172.17.11.246:16000");
		FileSpliter fileSpliter=new FileSpliter();
		List<FileContent> fileContents=fileSpliter.getFileContents();
		try {
			//获取hbase数据库的链接
			Connection connection = ConnectionFactory.createConnection(config);
			//createTable(connection);
			//根据表名获取表
			Table table = connection.getTable(TableName.valueOf("urltable"));
			//insert(table,fileContents);
			qu(table);
			//createTable(connection);
			//insert(table);

			//scantable(table,100);

			//table.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void scantable(Table table,int limit) throws IOException {
		int i=0;
		Scan scan = new Scan();

		//Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("201812120001")));
		//Filter filter = new PrefixFilter(Bytes.toBytes("01"));
		//scan.addColumn(Bytes.toBytes("url"),null);
		//scan.addColumn(Bytes.toBytes("links"),null);
		//scan.addColumn(Bytes.toBytes("title"),null);
		//scan.addColumn(Bytes.toBytes("content"),null);

		//Filter filter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("19"));

		//scan.setFilter(filter);

		ResultScanner rs = table.getScanner(scan);
		Result result1=rs.next();
		byte[] jn=result1.getValue(Bytes.toBytes("title"), Bytes.toBytes("title"));
		String title=Bytes.toString(jn);
		System.out.print(rs);

		for (Result result : rs) {
			if(i>limit) {
				break;
			}
			//byte[] jn=result.getValue(Bytes.toBytes("stuno"), Bytes.toBytes("year"));
			//System.out.println(Bytes.toString(jn));

			System.out.print("RowKey:["+Bytes.toString(result.getRow())+"]");
			System.out.print("\t");

			//Map<byte[],Map<byte[],byte[]>>
			Map map=result.getNoVersionMap();
			Set set = map.keySet();
			Iterator it = set.iterator();
			while(it.hasNext()) {
				byte[] cfkey = (byte[])it.next();
				System.out.print(Bytes.toString(cfkey)+":[");

				Map kvMap = (Map)map.get(cfkey);
				Set<Map.Entry> kvs = kvMap.entrySet();
				Iterator kvit = kvs.iterator();
				while(kvit.hasNext()) {
					Map.Entry<byte[],byte[]> me = (Map.Entry)kvit.next();
					System.out.print(Bytes.toString(me.getKey())+":"+Bytes.toString(me.getValue())+",");
				}
				System.out.print("]\t");
			}

			System.out.println("");
			i++;
		}
	}

	private static void insert(Table table,List<FileContent> fileContents) throws IOException {

		//put 'tablename','row','colfamily:colname','value'
		for(int i=1;i<=fileContents.size();i++){
			String index="r"+String.valueOf(i);
			System.out.println(index);
			Put put = new Put(Bytes.toBytes(index));
			FileContent fileContent=fileContents.get(i-1);
			String linkString="";
			for(String link:fileContent.getLinks()){
				linkString=linkString+link+",";
			}
			put.addColumn(Bytes.toBytes("url"),Bytes.toBytes("url"),Bytes.toBytes(fileContent.getUrl()));
			put.addColumn(Bytes.toBytes("links"),Bytes.toBytes("links"),Bytes.toBytes(linkString));
			put.addColumn(Bytes.toBytes("title"),Bytes.toBytes("title"),Bytes.toBytes(fileContent.getTitle()));
			put.addColumn(Bytes.toBytes("content"),Bytes.toBytes("content"),Bytes.toBytes(fileContent.getContent()));
			table.put(put);
		}
	}

	private static void createTable(Connection connection) throws IOException {
		//创建数据库管理对象
		Admin admin = connection.getAdmin();// hbase表管理类
		//创建表对象
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("page"));
		//创建列族对象
		HColumnDescriptor urlhcd = new HColumnDescriptor("url");
		HColumnDescriptor linkshcd = new HColumnDescriptor("links");
		HColumnDescriptor titlehcd = new HColumnDescriptor("title");
		HColumnDescriptor contenthcd = new HColumnDescriptor("content");
		//把列族加入表
		htd.addFamily(urlhcd);
		htd.addFamily(linkshcd);
		htd.addFamily(titlehcd);
		htd.addFamily(contenthcd);
		//创建表
		admin.createTable(htd);
		admin.close();
	}

	private static void q1(Table table) throws IOException {
		//创建get对象，构造方法传入rk
		Get get = new Get(Bytes.toBytes("数据库"));
		//通过Table的get方法获取结果集
		Result result = table.get(get);
		//根据列族，列名获取值
        for(Cell cell :result.rawCells()){
            String s1=Bytes.toString(CellUtil.cloneValue(result.rawCells()[1]));
            System.out.println(CellUtil.cloneValue(cell));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("url"), Bytes.toBytes("url"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("key"), Bytes.toBytes("key"))));
        }
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("title"), Bytes.toBytes("title"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("url"), Bytes.toBytes("url"))));
		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("key"), Bytes.toBytes("key"))));
        //System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("content"), Bytes.toBytes("content"))));
	}

	private static void qu(Table table) throws IOException {
        Scan scan = new Scan();
        //QualifierFilter filter1 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("key")));
        Filter filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("key")));
        scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "行键:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                                "列族:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                                "列名:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                                "值:" + new String(CellUtil.cloneValue(cell)));
            }
        }
    }


}

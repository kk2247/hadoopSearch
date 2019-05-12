
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author KGZ
 * @date 2018/12/24 15:44
 */

public class HbaseService {
    public List<Value> query(String key){
        System.setProperty("hadoop.home.dir", "C:\\hadoop-2.7.6");
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "172.17.11.250,172.17.11.251,172.17.11.252");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "172.17.11.246:16000");
        List<Value> values = null;
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            Table table = connection.getTable(TableName.valueOf("urltable"));
            values=qure(table,key);
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return values;
    }

    public List<Value> qure(Table table, String key) throws IOException {
        Scan scan = new Scan();
        //QualifierFilter filter1 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("key")));
        Filter filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("key")));
        scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        List<Value> values=new ArrayList<>();
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                Value value=new Value();
                value.setKey(new String(CellUtil.cloneQualifier(cell)));
                value.setValue(new String(CellUtil.cloneValue(cell)));
                values.add(value);
                /*System.out.println(
                        "行键:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                                "列族:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                                "列名:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                                "值:" + new String(CellUtil.cloneValue(cell)));*/
            }
        }
        return values;
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * @author KGZ
 * @date 2018/12/24 10:19
 */
public class Run{
   // public static String con="";
    public static void main(String[] args) throws Exception {
        Scanner scaner=new Scanner(System.in);
        //con=scaner.nextLine();
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "172.17.11.250,172.17.11.251,172.17.11.252");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "172.17.11.246:16000");
        Job job = Job.getInstance(conf,"hbasedemo");
        job.setJarByClass(Run.class);

        List<Scan> list = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setStartRow("r1".getBytes());
        scan.setStopRow("r400".getBytes());
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "page".getBytes());
        list.add(scan);

        //System.out.println(list.get(0));

        TableMapReduceUtil.initTableMapperJob(list,HbaseMapper.class, Text.class, Text.class, job);
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(Text.class);
        //job.setReducerClass(HbaseReducer.class);
        TableMapReduceUtil.initTableReducerJob("urltable", HbaseReducer.class, job);
        //job.setSortComparatorClass(ReadHbase.IntWritableDecreasingComparator.class);
        //FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\22478\\Desktop\\data\\out"));

        System.exit(job.waitForCompletion(true)==true?0:1);
    }
}

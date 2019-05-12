/**
 * @author KGZ
 * @date 2018/12/23 19:34
 */

import com.huaban.analysis.jieba.JiebaSegmenter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable.Comparator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import org.apache.thrift.transport.TFileTransport.truncableBufferedInputStream;

public class ReadHbase {

    static class HbaseMapper extends TableMapper<Text, IntWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value,Context context)
                throws IOException, InterruptedException {
            String con="";
            String title=Bytes.toString(CellUtil.cloneValue(value.rawCells()[2]));
            for(Cell cell :value.rawCells()){
                //context.write(new ImmutableBytesWritable("test".getBytes()),new Text(Bytes.toString(CellUtil.cloneQualifier(cell))+","+Bytes.toString(CellUtil.cloneValue(cell))));
                String text = Bytes.toString(CellUtil.cloneValue(cell));
                JiebaSegmenter segmenter = new JiebaSegmenter();
                List<String> strs=segmenter.sentenceProcess(text);
                for(int i=0;i<strs.size();i++){
                    if (strs.get(i).trim().equals("")==false){
                        if(strs.get(i).trim().equals(con)){
                            context.write(new Text(title.trim()),new IntWritable(1));
                        }
                    }

                }
//                System.out.println(segmenter.sentenceProcess(text));
            }
        }

    }
    static class HbaseReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));

            /*
            for(Text text:values){
                context.write(new Text(""), text);
                System.out.println(text);
            }*/
        }
    }


    static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare( WritableComparable a,WritableComparable b){
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    static class Run{
        public String con="";
        public static void main(String[] args) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "172.17.11.250,172.17.11.251,172.17.11.252");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.master", "172.17.11.246:16000");
            Job job = Job.getInstance(conf,"hbasedemo");
            job.setJarByClass(ReadHbase.class);

            List<Scan> list = new ArrayList<Scan>();
            Scan scan = new Scan();
            scan.setCaching(200);
            scan.setCacheBlocks(false);
            scan.setStartRow("r1".getBytes());
            scan.setStopRow("r400".getBytes());
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "page".getBytes());
            list.add(scan);

            //System.out.println(list.get(0));

            TableMapReduceUtil.initTableMapperJob(list, HbaseMapper.class, Text.class,IntWritable.class, job);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setReducerClass(HbaseReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setSortComparatorClass(IntWritableDecreasingComparator.class);
            FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\22478\\Desktop\\data\\out"));

            System.exit(job.waitForCompletion(true)==true?0:1);
        }

        public String getCon() {
            return con;
        }

        public void setCon(String con) {
            this.con = con;
        }
    }



}

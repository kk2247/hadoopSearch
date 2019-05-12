import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jruby.RubyProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author KGZ
 * @date 2018/12/24 10:19
 */
public class HbaseReducer extends TableReducer<Text,Text,Text> {
    private int id=1;

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context)
            throws IOException, InterruptedException {


        List<String>urls=new ArrayList<>();
        List<String>titles=new ArrayList<>();
        List<Integer>counts=new ArrayList<>();
        for(Text text:values){
            String urlAndTitle=String.valueOf(text);
            String url=urlAndTitle.split("####")[0];
            String title=urlAndTitle.split("####")[1];
            Put put = new Put(Bytes.toBytes(String.valueOf(id)));
            System.out.println(id);
            put.addColumn(Bytes.toBytes("key"),Bytes.toBytes("key"),Bytes.toBytes(String.valueOf(key)));
            put.addColumn(Bytes.toBytes("url"),Bytes.toBytes("url"),Bytes.toBytes(url));
            put.addColumn(Bytes.toBytes("title"),Bytes.toBytes("title"),Bytes.toBytes(title));
            id++;
            context.write(new Text(String.valueOf(id)),put);
        }
        /*
        //List<Text> myList= IteratorUtils.toList(values.iterator());
        for(int i=0;i<myList.size();i++){
            int count =1;
            String urlAndTitle=String.valueOf(myList.get(i));
            String url=urlAndTitle.split("####")[0];
            String title=urlAndTitle.split("####")[1];
            for(int j=i;j<myList.size();j++){
                String url1=String.valueOf(myList.get(j)).split("####")[0];
                if(url.equals(url1)){
                    myList.remove(j);
                    j--;
                    count++;
                }
            }
            urls.add(url);
            titles.add(title);
            counts.add(count);
        }
        Object[] objects=sort(urls,titles,counts);
        List<String> u= (List<String>) objects[0];
        List<String> t= (List<String>) objects[1];
        List<Integer> c= (List<Integer>) objects[2];
        for(int i=0;i<u.size();i++){
            Put put = new Put(Bytes.toBytes(String.valueOf(id)));
            System.out.println(id);
            put.addColumn(Bytes.toBytes("key"),Bytes.toBytes("key"),Bytes.toBytes(String.valueOf(key)));
            put.addColumn(Bytes.toBytes("url"),Bytes.toBytes("url"),Bytes.toBytes(u.get(i)));
            put.addColumn(Bytes.toBytes("title"),Bytes.toBytes("title"),Bytes.toBytes(t.get(i)));
            id++;
            context.write(new Text(String.valueOf(id)),put);
        }*/
//        for(Text val : values){
//
//            String urlAndTitle=String.valueOf(val);
//            String url=urlAndTitle.split("####")[0];
//            String title=urlAndTitle.split("####")[1];
//            int count =1;
//
//
//            put.addColumn(Bytes.toBytes("url"),Bytes.toBytes("url"),Bytes.toBytes(url));
//            put.addColumn(Bytes.toBytes("title"),Bytes.toBytes("title"),Bytes.toBytes(title));
//        }

    }

    public Object[] sort(List<String> url,List<String>title,List<Integer> counts){
        for(int i=0;i<counts.size();i++){
            for(int j=i;j<counts.size();j++){
                if(counts.get(i)<counts.get(j)){
                    String temp=url.get(j);
                    url.set(j,url.get(i));
                    url.set(i,temp);
                    temp=title.get(j);
                    title.set(j,title.get(i));
                    title.set(i,temp);
                    int te=counts.get(j);
                    counts.set(j,counts.get(i));
                    counts.set(i,te);
                }
            }
        }
        Object[] objects=new Object[3];
        objects[0]=url;
        objects[1]=title;
        objects[2]=counts;
        return  objects;
    }
}

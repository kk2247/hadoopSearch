import com.huaban.analysis.jieba.JiebaSegmenter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

/**
 * @author KGZ
 * @date 2018/12/24 10:19
 */
public class HbaseMapper extends TableMapper<Text, Text> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        //String con=Run.con;
        //String title= Bytes.toString(CellUtil.cloneValue(value.rawCells()[2]));
        String content=Bytes.toString(CellUtil.cloneValue(value.rawCells()[0]));
        String links=Bytes.toString(CellUtil.cloneValue(value.rawCells()[1]));
        String title=Bytes.toString(CellUtil.cloneValue(value.rawCells()[2]));
        String url=Bytes.toString(CellUtil.cloneValue(value.rawCells()[3]));
        JiebaSegmenter segmenter = new JiebaSegmenter();
        List<String> strs=segmenter.sentenceProcess(title);
        for(int i=0;i<strs.size();i++){
            if (strs.get(i).trim().equals("")==false){
                context.write(new Text(strs.get(i).trim()),new Text(url+"####"+title));
            }
        }
    }

}

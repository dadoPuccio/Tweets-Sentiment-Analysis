package batchLayer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Reducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    @Override
    protected void reduce(Text KeyWord, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int nPositive = 0;
        int nNegative = 0;
        for(IntWritable value : values){
            if(value.get() == 1){
                nPositive++;
            } else {
                nNegative++;
            }
        }

        Put put = new Put(Bytes.toBytes(KeyWord.toString()))
                .addColumn(Bytes.toBytes("sentiment_count"), Bytes.toBytes("nPositive"), Bytes.toBytes(nPositive))
                .addColumn(Bytes.toBytes("sentiment_count"), Bytes.toBytes("nNegative"), Bytes.toBytes(nNegative));

        System.out.println("[Batch Layer] Added " + KeyWord + " Pos:" + nPositive + " Neg:" + nNegative);
        context.write(null, put);
    }
}

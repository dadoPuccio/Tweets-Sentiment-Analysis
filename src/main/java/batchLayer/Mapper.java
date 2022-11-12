package batchLayer;

import classifier.SentimentClassifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;

public class Mapper extends TableMapper<Text, IntWritable> {

    private long startTimeStamp;
    private String[] keywords;
    private SentimentClassifier classifier;

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        startTimeStamp = conf.getLong("startTimeStamp", 0);
        System.out.println(startTimeStamp);
        keywords = conf.getStrings("keywords");

        try {
            File model = new File(context.getCacheFiles()[0].toString());
            classifier = new SentimentClassifier(model);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        long tupleTimeStamp = value.rawCells()[0].getTimestamp();
        if(tupleTimeStamp <= startTimeStamp){
            String text = new String(value.getValue(Bytes.toBytes("content"),Bytes.toBytes("text")));
            String sentiment = classifier.classify(text);
            // System.out.println(sentiment);
            for(String keyword : keywords){
                if(text.contains(keyword.toLowerCase()))
                    context.write(new Text(keyword), new IntWritable(Integer.parseInt(sentiment)));
            }
        }
    }
}

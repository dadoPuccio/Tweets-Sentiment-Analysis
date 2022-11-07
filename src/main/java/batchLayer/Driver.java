package batchLayer;

import servingLayer.ServingLayer;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.*;


public class Driver {

    private static final String MODEL_FILE_PATH = "./src/main/resources/SentimentClassifier.model";
    private static final int BATCH_LAYER_EXEC_WAIT = 5000; // further wait to simulate a slower batch layer

    public static void doWork(String[] keywords) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        ServingLayer.notifyBatchStart();

        Configuration conf = new Configuration();
        conf.setLong("startTimeStamp", System.currentTimeMillis());
        conf.setStrings("keywords", keywords);

        Job job = Job.getInstance(conf, "BatchLayer_SentimentAnalysis");
        job.setJarByClass(Driver.class);
        job.addCacheFile(new URI(MODEL_FILE_PATH));

        Scan scan =  new Scan();
        scan.addFamily(Bytes.toBytes("content"));
        scan.setCaching(500);

        TableMapReduceUtil.initTableMapperJob("batch_table",
                scan,
                Mapper.class,
                Text.class,
                IntWritable.class,
                job);

        TableMapReduceUtil.initTableReducerJob("batch_view_table",
                Reducer.class,
                job);

        // job.setNumReduceTasks(1);

        boolean res = job.waitForCompletion(false);
        if(!res){
            throw new RuntimeException("Batch Layer computation failed");
        }

        Thread.sleep(BATCH_LAYER_EXEC_WAIT);

        ServingLayer.notifyBatchEnd();
    }

    public static void startBatchLayer(String[] keywords) throws Exception {
        doWork(keywords);
    }


}

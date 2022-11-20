package servingLayer;

import batchLayer.Driver;
import gui.MainSceneController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import speedLayer.Topology;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ServingLayer implements Runnable {

    private static Connection HBaseConnection;

    private static final String SPEED_TABLE_NAME = "speed_table";
    private static final String SYNC_TABLE_NAME = "sync_table";
    private static final String BATCH_TABLE_NAME = "batch_table";
    private static final String BATCH_VIEW_TABLE_NAME = "batch_view_table";

    private static Table speedTable;
    private static Table syncTable;
    private static Table batchTable;
    private static Table batchViewTable;

    private final String[] keywords;
    private static boolean needToStop = false;
    private static MainSceneController viewController;

    public ServingLayer(String[] keywords, MainSceneController viewController) {
        this.keywords = keywords;
        ServingLayer.viewController = viewController;
    }

    @Override
    public void run() {
        try {
            main(keywords);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] keywords) throws Exception {
        initTables();

        Topology.startSpeedLayer(keywords);
        while (!needToStop)
            Driver.startBatchLayer(keywords);

        Topology.stopSpeedLayer();
        HBaseConnection.close();
    }

    public static void stop(){
        needToStop = true;
    }

    public static void initTables() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HBaseConnection = ConnectionFactory.createConnection(configuration);
        Admin admin = HBaseConnection.getAdmin();

        cleanUpOldResults(admin);

        if (!admin.tableExists(TableName.valueOf(SPEED_TABLE_NAME))) {
            TableDescriptor speedTableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(SPEED_TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("tweet_sentiments")).build())
                    .build();

            admin.createTable(speedTableDescriptor);
        }

        if (!admin.tableExists(TableName.valueOf(SYNC_TABLE_NAME))) {
            TableDescriptor syncTableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(SYNC_TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("batch_timestamps")).build())
                    .build();

            admin.createTable(syncTableDescriptor);

            syncTable = HBaseConnection.getTable(TableName.valueOf(SYNC_TABLE_NAME));

            Put previous_start_timestamp = new Put(Bytes.toBytes("previous_start_timestamp"), 0).addColumn(Bytes.toBytes("batch_timestamps"), Bytes.toBytes(""), Bytes.toBytes(""));
            Put start_timestamp = new Put(Bytes.toBytes("start_timestamp"), 0).addColumn(Bytes.toBytes("batch_timestamps"), Bytes.toBytes(""), Bytes.toBytes(""));
            Put end_timestamp = new Put(Bytes.toBytes("end_timestamp"), 0).addColumn(Bytes.toBytes("batch_timestamps"), Bytes.toBytes(""), Bytes.toBytes(""));

            syncTable.put(previous_start_timestamp);
            syncTable.put(start_timestamp);
            syncTable.put(end_timestamp);
        }

        if (!admin.tableExists(TableName.valueOf(BATCH_TABLE_NAME))) {
            TableDescriptor batchTableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(BATCH_TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("content")).build())
                    .build();

            admin.createTable(batchTableDescriptor);
        }

        if (!admin.tableExists(TableName.valueOf(BATCH_VIEW_TABLE_NAME))) {
            TableDescriptor batchViewTableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(BATCH_VIEW_TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("sentiment_count")).build())
                    .build();

            admin.createTable(batchViewTableDescriptor);
        }

        speedTable = HBaseConnection.getTable(TableName.valueOf(SPEED_TABLE_NAME));
        syncTable = HBaseConnection.getTable(TableName.valueOf(SYNC_TABLE_NAME));
        batchTable = HBaseConnection.getTable(TableName.valueOf(BATCH_TABLE_NAME));
        batchViewTable = HBaseConnection.getTable(TableName.valueOf(BATCH_VIEW_TABLE_NAME));
    }

    public static void addSpeedTableEntry(String TweetID, String KeyWord, String Sentiment){
        String rowKey = TweetID + " " + KeyWord;
        Put put = new Put(Bytes.toBytes(rowKey))
                .addColumn(Bytes.toBytes("tweet_sentiments"), Bytes.toBytes("KeyWord"), Bytes.toBytes(KeyWord))
                .addColumn(Bytes.toBytes("tweet_sentiments"), Bytes.toBytes("Sentiment"), Bytes.toBytes(Sentiment));
        try{
            speedTable.put(put);
            viewController.updateRealTimeView();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void addBatchTableEntry(String TweetID, String Text){
        Put put = new Put(Bytes.toBytes(TweetID))
                .addColumn(Bytes.toBytes("content"), Bytes.toBytes("text"), Bytes.toBytes(Text));
        try {
            batchTable.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Long notifyBatchStart() throws IOException {

        long last_batch_start_timestamp = syncTable.get(new Get(Bytes.toBytes("start_timestamp"))).rawCells()[0].getTimestamp();

        Put previous_start_timestamp = new Put(Bytes.toBytes("previous_start_timestamp"), last_batch_start_timestamp).addColumn(Bytes.toBytes("batch_timestamps"), Bytes.toBytes(""), Bytes.toBytes(""));
        Put start_timestamp_put = new Put(Bytes.toBytes("start_timestamp")).addColumn(Bytes.toBytes("batch_timestamps"),  Bytes.toBytes(""), Bytes.toBytes(""));

        try {
            syncTable.put(previous_start_timestamp);
            syncTable.put(start_timestamp_put);

            viewController.updateBatchView();
            viewController.updateRealTimeView();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return start_timestamp_put.getTimestamp();
    }


    public static void notifyBatchEnd(){
        Put end_timestamp = new Put(Bytes.toBytes("end_timestamp")).addColumn(Bytes.toBytes("batch_timestamps"), Bytes.toBytes(""),  Bytes.toBytes(""));
        try {
            syncTable.put(end_timestamp);

            viewController.updateBatchView();
            viewController.updateRealTimeView();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static HashMap<String, HashMap<String, Integer>> getSpeedLayerCounts() throws IOException {
        HashMap<String, HashMap<String, Integer>> counts = new HashMap<>();

        long prev_batch_start_time = syncTable.get(new Get(Bytes.toBytes("previous_start_timestamp"))).rawCells()[0].getTimestamp();
        long batch_start_time = syncTable.get(new Get(Bytes.toBytes("start_timestamp"))).rawCells()[0].getTimestamp();
        long batch_end_time = syncTable.get(new Get(Bytes.toBytes("end_timestamp"))).rawCells()[0].getTimestamp();

        long expiration_threshold = batch_start_time > batch_end_time ? prev_batch_start_time : batch_start_time;

        expiration_threshold += 2000; // in order to compensate for delay of speed layer

        Scan scan = new Scan().addFamily(Bytes.toBytes("tweet_sentiments")).setTimeRange(expiration_threshold, Long.MAX_VALUE);
        ResultScanner resultScanner = speedTable.getScanner(scan);

        Result res = resultScanner.next();
        while(res != null){
            String keyword = Bytes.toString(res.getValue(Bytes.toBytes("tweet_sentiments"), Bytes.toBytes("KeyWord")));
            String sentiment = Bytes.toString(res.getValue(Bytes.toBytes("tweet_sentiments"), Bytes.toBytes("Sentiment")));

            if(!counts.containsKey(keyword)) {
                HashMap<String, Integer> emptyCounts = new HashMap<>();
                emptyCounts.put("positive", 0);
                emptyCounts.put("negative", 0);
                counts.put(keyword, emptyCounts);
            }

            if(sentiment.equals("1")){
                counts.get(keyword).merge("positive", 1, Integer::sum);
            } else if(sentiment.equals("0")){
                counts.get(keyword).merge("negative", 1, Integer::sum);
            } else {
                throw new RuntimeException("Speed Table has some invalid elements");
            }

            res = resultScanner.next();
        }

        return counts;
    }

    public static HashMap<String, HashMap<String, Integer>> getBatchLayerCounts() throws IOException {
        HashMap<String, HashMap<String, Integer>> counts = new HashMap<>();

        Scan scan = new Scan().addFamily(Bytes.toBytes("sentiment_count"));
        ResultScanner resultScanner = batchViewTable.getScanner(scan);

        Result res = resultScanner.next();
        while(res != null){
            String keyword = Bytes.toString(res.getRow());
            int nPositive = Bytes.toInt(res.getValue(Bytes.toBytes("sentiment_count"), Bytes.toBytes("nPositive")));
            int nNegative = Bytes.toInt(res.getValue(Bytes.toBytes("sentiment_count"), Bytes.toBytes("nNegative")));

            HashMap<String, Integer> keyCounts = new HashMap<>();
            keyCounts.put("positive", nPositive);
            keyCounts.put("negative", nNegative);
            counts.put(keyword, keyCounts);

            res = resultScanner.next();
        }

        return counts;
    }

    private static void cleanUpOldResults(Admin admin) throws IOException {
        admin.disableTable(TableName.valueOf(SPEED_TABLE_NAME));
        admin.deleteTable(TableName.valueOf(SPEED_TABLE_NAME));
        admin.disableTable(TableName.valueOf(SYNC_TABLE_NAME));
        admin.deleteTable(TableName.valueOf(SYNC_TABLE_NAME));
        admin.disableTable(TableName.valueOf(BATCH_TABLE_NAME));
        admin.deleteTable(TableName.valueOf(BATCH_TABLE_NAME));
        admin.disableTable(TableName.valueOf(BATCH_VIEW_TABLE_NAME));
        admin.deleteTable(TableName.valueOf(BATCH_VIEW_TABLE_NAME));
    }

    private static void deleteExpiredEntriesSpeedTable() throws IOException {
        /* Another approach: deletes entries from the speed table when a batch finishes */
        long batchLayerStartTimeStamp = syncTable.get(new Get(Bytes.toBytes("start_timestamp"))).rawCells()[0].getTimestamp();

        Scan scan = new Scan().setTimeRange(0, batchLayerStartTimeStamp);
        ResultScanner resultScanner = speedTable.getScanner(scan);

        List<Delete> listOfDelete = new ArrayList<>();

        Result res = resultScanner.next();
        while(res != null){
            listOfDelete.add(new Delete(res.getRow()));
            res = resultScanner.next();
        }

        speedTable.delete(listOfDelete);
    }
}

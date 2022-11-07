package servingLayer;

import batchLayer.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import speedLayer.Topology;

import java.io.IOException;

public class ServingLayer {

    private static final String SPEED_TABLE_NAME = "speed_table";
    private static final String SYNC_TABLE_NAME = "sync_table";
    private static final String BATCH_TABLE_NAME = "batch_table";
    private static final String BATCH_VIEW_TABLE_NAME = "batch_view_table";

    private static Table speedTable;
    private static Table syncTable;
    private static Table batchTable;
    private static Table batchViewTable;

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection HBaseConnection = ConnectionFactory.createConnection(configuration);
        Admin admin = HBaseConnection.getAdmin();

        if(!admin.tableExists(TableName.valueOf(SPEED_TABLE_NAME))){
            TableDescriptor speedTableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(SPEED_TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("tweet_sentiments")).build())
                    .build();

            admin.createTable(speedTableDescriptor);
        }

        if(!admin.tableExists(TableName.valueOf(SYNC_TABLE_NAME))){
            TableDescriptor syncTableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(SYNC_TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("batch_timestamps")).build())
                    .build();

            admin.createTable(syncTableDescriptor);

            syncTable = HBaseConnection.getTable(TableName.valueOf(SYNC_TABLE_NAME));

            Put start_timestamp = new Put(Bytes.toBytes("start_timestamp"), 0).addColumn(Bytes.toBytes("batch_timestamps"),  Bytes.toBytes(""), Bytes.toBytes(""));
            Put end_timestamp = new Put(Bytes.toBytes("end_timestamp"), 0).addColumn(Bytes.toBytes("batch_timestamps"), Bytes.toBytes(""),  Bytes.toBytes(""));

            syncTable.put(start_timestamp);
            syncTable.put(end_timestamp);
        }

        if(!admin.tableExists(TableName.valueOf(BATCH_TABLE_NAME))){
            TableDescriptor batchTableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(BATCH_TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("content")).build())
                    .build();

            admin.createTable(batchTableDescriptor);
        }

        if(!admin.tableExists(TableName.valueOf(BATCH_VIEW_TABLE_NAME))){
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

        String[] keywords = {"Google", "Apple", "Musk"};

        // Topology.startSpeedLayer(keywords);

        int batchIterations = 0;
        while(batchIterations < 5){
            Driver.startBatchLayer(keywords);
            batchIterations++;
        }

        // Topology.stopSpeedLayer();
        HBaseConnection.close();
    }

    public static void addSpeedTableEntry(String TweetID, String KeyWord, String Sentiment){
        String rowKey = TweetID + " " + KeyWord;
        Put put = new Put(Bytes.toBytes(rowKey))
                .addColumn(Bytes.toBytes("tweet_sentiments"), Bytes.toBytes("KeyWord"), Bytes.toBytes(KeyWord))
                .addColumn(Bytes.toBytes("tweet_sentiments"), Bytes.toBytes("Sentiment"), Bytes.toBytes(Sentiment));
        try{
            speedTable.put(put);
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

    public static void notifyBatchStart(){
        Put start_timestamp = new Put(Bytes.toBytes("start_timestamp")).addColumn(Bytes.toBytes("batch_timestamps"),  Bytes.toBytes(""), Bytes.toBytes(""));
        try {
            syncTable.put(start_timestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void notifyBatchEnd(){
        Put end_timestamp = new Put(Bytes.toBytes("end_timestamp")).addColumn(Bytes.toBytes("batch_timestamps"), Bytes.toBytes(""),  Bytes.toBytes(""));
        try {
            syncTable.put(end_timestamp);
            deleteExpiredEntriesSpeedTable();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void deleteExpiredEntriesSpeedTable() throws IOException {
        long batchLayerStartTimeStamp = syncTable.get(new Get(Bytes.toBytes("start_timestamp"))).rawCells()[0].getTimestamp();

        Scan scan = new Scan().setTimeRange(0, batchLayerStartTimeStamp);
        ResultScanner resultScanner = speedTable.getScanner(scan);

        Result res = resultScanner.next();
        while(res != null){
            speedTable.delete(new Delete(res.getRow()));
            res = resultScanner.next();
        }
    }


}

package servingLayer;

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

    private static Connection HBaseConnection;
    private static Table speedTable;
    private static Table syncTable;
    private static Table batchTable;
    private static Table batchViewTable;

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        HBaseConnection = ConnectionFactory.createConnection(configuration);
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
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("column_family")).build())
                    .build();

            admin.createTable(batchTableDescriptor);
        }

        if(!admin.tableExists(TableName.valueOf(BATCH_VIEW_TABLE_NAME))){
            TableDescriptor batchViewTableDescriptor = TableDescriptorBuilder
                    .newBuilder(TableName.valueOf(BATCH_VIEW_TABLE_NAME))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("column_family")).build())
                    .build();

            admin.createTable(batchViewTableDescriptor);
        }

        speedTable = HBaseConnection.getTable(TableName.valueOf(SPEED_TABLE_NAME));
        syncTable = HBaseConnection.getTable(TableName.valueOf(SYNC_TABLE_NAME));
        batchTable = HBaseConnection.getTable(TableName.valueOf(BATCH_TABLE_NAME));
        batchViewTable = HBaseConnection.getTable(TableName.valueOf(BATCH_VIEW_TABLE_NAME));

        String[] keywords = {"Google", "Apple", "Musk"};

        Topology.startSpeedLayer(keywords);
        Thread.sleep(300000);
        Topology.stopSpeedLayer();

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



}

package speedLayer;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


public class Topology{

    private static final String CREDENTIALS_FILE_PATH = "./src/main/resources/credentials.json";
    private static final String MODEL_FILE_PATH = "./src/main/resources/SentimentClassifier.model";
    private static final String SUPPLEMENTARY_INPUT_FILE = "./src/main/resources/test.csv";

    private static LocalCluster speedLayerCluster;

    public static void startSpeedLayer(String[] keywords) throws Exception {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("TwitterSpout", new TwitterSpout(CREDENTIALS_FILE_PATH, keywords, 1000), 1);
        topologyBuilder.setSpout("FileSpout", new FileSpout(SUPPLEMENTARY_INPUT_FILE, keywords), 1);

        topologyBuilder.setBolt("ParserBolt", new ParserBolt(keywords), 5)
                .shuffleGrouping("TwitterSpout");
        topologyBuilder.setBolt("SentimentBolt", new SentimentBolt(MODEL_FILE_PATH), 5)
                .shuffleGrouping("ParserBolt")
                .shuffleGrouping("FileSpout");

        topologyBuilder.setBolt("SpeedTableBolt", new SpeedTableBolt(), 5)
                .shuffleGrouping("SentimentBolt");
        topologyBuilder.setBolt("BatchTableBolt", new BatchTableBolt(), 1)
                .shuffleGrouping("ParserBolt")
                .shuffleGrouping("FileSpout");

        Config config = new Config();
        speedLayerCluster = new LocalCluster();
        speedLayerCluster.submitTopology("SpeedLayer", config, topologyBuilder.createTopology());
    }

    public static void stopSpeedLayer(){
        speedLayerCluster.shutdown();
    }

}

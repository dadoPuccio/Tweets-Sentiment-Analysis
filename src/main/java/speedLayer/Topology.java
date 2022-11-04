package speedLayer;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


public class Topology{

    private static final String CREDENTIALS_FILE_PATH = "./src/main/resources/credentials.json";
    private static final String MODEL_FILE_PATH = "./src/main/resources/SentimentClassifier.model";

    private static LocalCluster speedLayerCluster;

    public static void startSpeedLayer(String[] keywords) throws Exception {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("TwitterSpout", new TwitterSpout(CREDENTIALS_FILE_PATH, keywords));
        topologyBuilder.setBolt("ParserBolt", new ParserBolt(keywords), 2)
                .shuffleGrouping("TwitterSpout");
        topologyBuilder.setBolt("SentimentBolt", new SentimentBolt(MODEL_FILE_PATH), 2)
                .shuffleGrouping("ParserBolt");
        topologyBuilder.setBolt("SpeedTableBolt", new SpeedTableBolt(), 2)
                .shuffleGrouping("SentimentBolt");

        Config config = new Config();
        speedLayerCluster = new LocalCluster();
        speedLayerCluster.submitTopology("SpeedLayer", config, topologyBuilder.createTopology());
    }

    public static void stopSpeedLayer(){
        speedLayerCluster.shutdown();
    }

}

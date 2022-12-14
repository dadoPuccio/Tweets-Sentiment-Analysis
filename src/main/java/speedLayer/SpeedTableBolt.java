package speedLayer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import servingLayer.ServingLayer;

import java.util.Map;

public class SpeedTableBolt extends BaseRichBolt {


    @Override
    public void prepare(Map<String, Object> topologyConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        String ID = input.getStringByField("ID");
        String KeyWord = input.getStringByField("KeyWord");
        String Sentiment = input.getStringByField("Sentiment");

        System.out.println("[Speed Layer] Added " + KeyWord + " " + Sentiment);
        ServingLayer.addSpeedTableEntry(ID, KeyWord, Sentiment);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields here, as the output is written into SpeedTable
    }

}

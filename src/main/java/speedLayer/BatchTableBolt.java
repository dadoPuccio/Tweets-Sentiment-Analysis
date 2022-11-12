package speedLayer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import servingLayer.ServingLayer;

import java.util.Map;

public class BatchTableBolt extends BaseRichBolt {
    @Override
    public void prepare(Map<String, Object> topologyConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {
        String ID = input.getStringByField("ID");
        String Content = input.getStringByField("Content");

        ServingLayer.addBatchTableEntry(ID, Content);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

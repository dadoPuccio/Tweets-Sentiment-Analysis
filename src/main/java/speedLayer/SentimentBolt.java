package speedLayer;

import classifier.SentimentClassifier;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class SentimentBolt extends BaseRichBolt {

    private OutputCollector collector;
    private final String MODEL_FILE_PATH;
    private SentimentClassifier classifier;

    public SentimentBolt(String model_file_path) {
        MODEL_FILE_PATH = model_file_path;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        File model = new File(MODEL_FILE_PATH);
        try {
            classifier = new SentimentClassifier(model);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        String ID = input.getStringByField("ID");
        String Content = input.getStringByField("Content");
        String Sentiment = classifier.classify(Content);

        // System.out.println(Content + "\t" + Sentiment);

        for(String KeyWord : input.getStringByField("KeyWords").split(",")) {
            collector.emit(new Values(ID, KeyWord, Sentiment));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID","KeyWord", "Sentiment"));
    }
}

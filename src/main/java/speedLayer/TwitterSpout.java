package speedLayer;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

public class TwitterSpout extends BaseRichSpout {

    private final String BEARER_TOKEN;
    private final String[] keywords;
    private FilteredStream filteredStream;

    private SpoutOutputCollector collector;

    public TwitterSpout(String credentials_file_path, String[] keywords) throws IOException, ParseException {
        JSONParser jsonParser = new JSONParser();
        FileReader inputFile = new FileReader(credentials_file_path);

        JSONObject credentials = (JSONObject) jsonParser.parse(inputFile);

        BEARER_TOKEN = credentials.get("bearerToken").toString();

        this.keywords = keywords;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        filteredStream = new FilteredStream(BEARER_TOKEN, keywords);
        try {
            filteredStream.setupRules();
            filteredStream.connectStream();
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public void nextTuple() {
        org.json.JSONObject tweet = filteredStream.getTweet();
        if(tweet == null)
            Utils.sleep(50);
        else
            collector.emit(new Values(tweet));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Tweet"));
    }

}

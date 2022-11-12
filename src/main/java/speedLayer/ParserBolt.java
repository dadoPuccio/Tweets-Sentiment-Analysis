package speedLayer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

import java.util.Map;

public class ParserBolt extends BaseRichBolt {

    private OutputCollector collector;
    private final String[] keywords;

    public ParserBolt(String[] keywords) {
        this.keywords = keywords;
    }

    @Override
    public void prepare(Map<String, Object> topologyConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        JSONObject tweet = (JSONObject) input.getValueByField("Tweet");
        String content = tweet.get("text").toString().toLowerCase();
        String ID = tweet.get("id").toString();
        // System.out.println(content);

        StringBuilder tweetKeywords = new StringBuilder();
        for (String keyword : keywords){
            if(content.contains(keyword.toLowerCase())) {
                tweetKeywords.append(keyword);
                tweetKeywords.append(",");
            }
        }
        if(!tweetKeywords.toString().isEmpty())
            collector.emit(new Values(ID, content, tweetKeywords.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "Content", "KeyWords"));
    }
}

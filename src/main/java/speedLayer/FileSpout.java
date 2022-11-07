package speedLayer;

import com.aliasi.util.CommaSeparatedValues;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class FileSpout extends BaseRichSpout {

    private final String[][] input_rows;


    private SpoutOutputCollector collector;
    private final String[] keywords;

    public FileSpout(String input_file, String[] keywords) throws IOException {
        this.keywords = keywords;
        File file = new File(input_file);
        CommaSeparatedValues csv = new CommaSeparatedValues(file, "UTF-8");
        input_rows = csv.getArray();
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String ID, content;

        for(String[] row : input_rows){
            if(row.length == 7) {
                ID = row[1];
                content = row[5].toLowerCase();

                StringBuilder tweetKeywords = new StringBuilder();
                for(String keyword : keywords){
                    if(content.contains(keyword.toLowerCase())){
                        tweetKeywords.append(keyword);
                        tweetKeywords.append(",");
                    }
                }

                if(!tweetKeywords.toString().isEmpty())
                    collector.emit(new Values(ID, content, tweetKeywords.toString()));

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "Content", "KeyWords"));
    }
}

package speedLayer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TwitterSpout extends BaseRichSpout {

    private final String BEARER_TOKEN;
    private final String[] keywords;
    private final int N_TWEETS;

    private SpoutOutputCollector collector;

    public TwitterSpout(String credentials_file_path, String[] keywords, int nTweets) throws IOException, ParseException {
        JSONParser jsonParser = new JSONParser();
        FileReader inputFile = new FileReader(credentials_file_path);

        JSONObject credentials = (JSONObject) jsonParser.parse(inputFile);

        this.BEARER_TOKEN = credentials.get("bearerToken").toString();

        this.keywords = keywords;
        this.N_TWEETS = nTweets;
    }

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        try {
            setupRules();
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void nextTuple() {
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        try{
            URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream");

            HttpGet httpGet = new HttpGet(uriBuilder.build());
            httpGet.setHeader("Authorization", String.format("Bearer %s", BEARER_TOKEN));

            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();

            if (null != entity) {
                BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));

                String line;
                org.json.JSONObject jsonTweet;

                int counter = 0;
                while (counter < N_TWEETS) {
                    line = reader.readLine();
                    if(line != null && !line.isEmpty()) {
                        jsonTweet = new org.json.JSONObject(line);
                        try {
                            org.json.JSONObject tweet = (org.json.JSONObject) jsonTweet.get("data");
                            collector.emit(new Values(tweet));
                        } catch (JSONException e){
                            System.out.println("[TwitterSpout] Connection Error");
                            counter = N_TWEETS;
                        }

                        counter += 1;

                    } else {
                        System.out.println("[TwitterSpout] Blank Line! - Counter: " + counter);
                    }
                }
            }
            httpGet.releaseConnection();
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Tweet"));
    }

    private void setupRules() throws IOException, URISyntaxException {
        /* Helper method to setup rules before streaming data */
        List<String> existingRules = getRules();
        if (existingRules.size() > 0) {
            deleteRules(existingRules);
        }
        createRules();
    }

    private void createRules() throws URISyntaxException, IOException {
        /*  Helper method to create rules for filtering */
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", BEARER_TOKEN));
        httpPost.setHeader("content-type", "application/json");

        StringBuilder query = new StringBuilder();
        for(int i=0; i < (keywords.length - 1); i++){
            query.append(keywords[i]);
            query.append(" OR ");
        }
        query.append(keywords[keywords.length - 1]);

        StringEntity body = new StringEntity("{\"add\": [{\"value\": \"("+ query+") lang:en\"}]}");
        System.out.println("[TwitterSpout] " + query);
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            System.out.println("[TwitterSpout] " + EntityUtils.toString(entity, "UTF-8"));
        }
    }


    private List<String> getRules() throws URISyntaxException, IOException {
        /* Helper method to get existing rules */
        List<String> rules = new ArrayList<>();
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", BEARER_TOKEN));
        httpGet.setHeader("content-type", "application/json");
        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            org.json.JSONObject json = new org.json.JSONObject(EntityUtils.toString(entity, "UTF-8"));
            if (json.length() > 1) {
                JSONArray array = (JSONArray) json.get("data");
                for (int i = 0; i < array.length(); i++) {
                    org.json.JSONObject jsonObject = (org.json.JSONObject) array.get(i);
                    rules.add(jsonObject.getString("id"));
                }
            }
        }
        return rules;
    }

    private void deleteRules(List<String> existingRules) throws URISyntaxException, IOException {
        /* Helper method to delete rules */
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", BEARER_TOKEN));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    private String getFormattedString(String string, List<String> ids) {
        StringBuilder sb = new StringBuilder();
        if (ids.size() == 1) {
            return String.format(string, "\"" + ids.get(0) + "\"");
        } else {
            for (String id : ids) {
                sb.append("\"").append(id).append("\"").append(",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }
}

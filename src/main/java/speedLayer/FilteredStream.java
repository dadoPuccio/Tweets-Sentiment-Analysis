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
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class FilteredStream {

    private final String BEARER_TOKEN;
    private final String[] keywords;
    private final LinkedBlockingQueue<JSONObject> tweetsQueue;

    FilteredStream(String bearerToken, String[] keywords){
        if(bearerToken != null){
            BEARER_TOKEN = bearerToken;
            this.keywords = keywords;
            tweetsQueue = new LinkedBlockingQueue<>(1000);
        } else {
            throw new RuntimeException("Bearer Token is null");
        }
    }

    public void setupRules() throws IOException, URISyntaxException {
        /* Helper method to setup rules before streaming data */
        List<String> existingRules = getRules();
        if (existingRules.size() > 0) {
            deleteRules(existingRules);
        }
        createRules();
    }

    public void connectStream() throws IOException, URISyntaxException {
        /* This method calls the filtered stream endpoint and streams Tweets from it */

        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream");

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", BEARER_TOKEN));

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
            String line = reader.readLine();
            JSONObject jsonTweet = new JSONObject(line);

            int counter = 0;
            while (counter < 1000) {
                JSONObject tweet = (JSONObject) jsonTweet.get("data");
                tweetsQueue.offer(tweet);

                counter += 1;
                line = reader.readLine();
                if(line.isEmpty())
                    counter = 1000;
                else
                    jsonTweet = new JSONObject(line);
            }
        }
    }

    public JSONObject getTweet(){
        return tweetsQueue.poll();
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
        System.out.println(body);
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
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
            JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
            if (json.length() > 1) {
                JSONArray array = (JSONArray) json.get("data");
                for (int i = 0; i < array.length(); i++) {
                    JSONObject jsonObject = (JSONObject) array.get(i);
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


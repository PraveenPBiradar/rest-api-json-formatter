package com.app.rest.data.agrlogsrest;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

@org.springframework.web.bind.annotation.RestController
public class RestController {

    @PostMapping("/callApi/{client}")
    public String postCall(@PathVariable String client) throws IOException {
        final String POST_PARAMS = "{\n" +
                "    \"size\": 0,\n" +
                "    \"query\": {\n" +
                "        \"bool\": {\n" +
                "            \"must\": [\n" +
                "                {\n" +
                "                    \"query_string\": {\n" +
                "                        \"query\": \"application:agrippa-data AND env:prod AND client:" + client + "\",\n" +
                "                        \"analyze_wildcard\": true\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"range\": {\n" +
                "                        \"@timestamp\": {\n" +
                "                            \"gte\": 1621235325971,\n" +
                "                            \"lte\": 1621236225971,\n" +
                "                            \"format\": \"epoch_millis\"\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            ],\n" +
                "            \"must_not\": []\n" +
                "        }\n" +
                "    },\n" +
                "    \"_source\": {\n" +
                "        \"excludes\": []\n" +
                "    },\n" +
                "    \"aggs\": {\n" +
                "        \"3\": {\n" +
                "            \"terms\": {\n" +
                "                \"field\": \"endpoint\",\n" +
                "                \"size\": 20,\n" +
                "                \"order\": {\n" +
                "                    \"_count\": \"desc\"\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        //System.out.println(POST_PARAMS);
        URL obj = new URL("https://agrippa-logs.nexus.bazaarvoice.com/elasticsearch/requests-*/_search");
        HttpURLConnection postConnection = (HttpURLConnection) obj.openConnection();
        postConnection.setRequestMethod("POST");
        postConnection.setRequestProperty("kbn-xsrf", "reporting");
        postConnection.setRequestProperty("Content-Type", "application/json");


        postConnection.setDoOutput(true);
        OutputStream os = postConnection.getOutputStream();
        os.write(POST_PARAMS.getBytes());
        os.flush();
        os.close();


        int responseCode = postConnection.getResponseCode();
        System.out.println("POST Response Code :  " + responseCode);
        System.out.println("POST Response Message : " + postConnection.getResponseMessage());
        StringBuilder response = null;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    postConnection.getInputStream()));
            String inputLine;
            response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            System.out.println(response.toString());
        } else {
            System.out.println("POST NOT WORKED");
        }

        JSONObject objRes = new JSONObject(response.toString());
        JSONObject objAgr = objRes.getJSONObject("aggregations");
        JSONObject objResAgr = objAgr.getJSONObject("3");
        System.out.println(objResAgr.toString());
        String endpoint = "";
        int count;
        Map<String, Integer> map = new HashMap<String, Integer>();
        List list = new ArrayList<>();

        JSONArray arr = objResAgr.getJSONArray("buckets"); // notice that `"posts": [...]`
        Map<String, Integer> largeMap = new HashMap<>();
        for (int i = 0; i < arr.length(); i++) {
            JSONObject jsonobject = arr.getJSONObject(i);
            count = arr.getJSONObject(i).getInt("doc_count");
            endpoint = arr.getJSONObject(i).getString("key");
            System.out.println(endpoint);
            System.out.println(count);

            if (arr.length() == 1) {
                try (PrintWriter writer = new PrintWriter(new File("testunique.csv"))) {

                    StringBuilder sb = new StringBuilder();
                    sb.append("endpoint,");
                    sb.append("value,");
                    sb.append('\n');

                    sb.append(endpoint);
                    sb.append(',');
                    sb.append(count);
                    sb.append('\n');

                    writer.write(sb.toString());

                    System.out.println("done!");

                } catch (FileNotFoundException e) {
                    System.out.println(e.getMessage());
                }
            } else if (arr.length() > 1) {
                largeMap.put(endpoint, count);
            }
        }
        System.out.println(largeMap.entrySet());
        appendCsvForLargeData(largeMap);
        return response.toString();
    }

    private void appendCsvForLargeData(Map<String, Integer> largeMap) throws FileNotFoundException {
        try (PrintWriter writerLarge = new PrintWriter(new File("testuniqueLarge.csv"))) {
            Iterator<Map.Entry<String, Integer>> itr = largeMap.entrySet().iterator();
            StringBuilder sbLarge = new StringBuilder();
            /*sbLarge.append("endpoint,");
            sbLarge.append("value,");
            sbLarge.append('\n');*/
            while (itr.hasNext()) {
                Map.Entry<String, Integer> entry = itr.next();


                sbLarge.append(entry.getKey());
                sbLarge.append(',');
                sbLarge.append(entry.getValue());
                sbLarge.append('\n');

                writerLarge.write(sbLarge.toString());

                System.out.println("done!");
            }
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }

    }
}

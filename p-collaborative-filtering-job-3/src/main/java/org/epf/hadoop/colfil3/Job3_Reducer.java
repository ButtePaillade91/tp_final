package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.naming.Context;
import java.io.IOException;
import java.util.*;

public class Job3_Reducer extends Reducer<Text, Text, Text, Text> {
    private Text recommendations = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Collect and sort recommendations by number of common relations
        TreeMap<Integer, List<String>> sortedRecommendations = new TreeMap<>(Collections.reverseOrder());

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String recommendedUser = parts[0];
            int commonFriends = Integer.parseInt(parts[1]);

            sortedRecommendations.putIfAbsent(commonFriends, new ArrayList<>());
            sortedRecommendations.get(commonFriends).add(recommendedUser);
        }

        // Collect the top 5 recommendations
        StringBuilder topRecommendations = new StringBuilder();
        int count = 0;
        for (Map.Entry<Integer, List<String>> entry : sortedRecommendations.entrySet()) {
            for (String user : entry.getValue()) {
                if (count >= 5) break;
                if (topRecommendations.length() > 0) {
                    topRecommendations.append(",");
                }
                topRecommendations.append(user).append(":").append(entry.getKey());
                count++;
            }
            if (count >= 5) break;
        }

        recommendations.set(topRecommendations.toString());
        context.write(key, recommendations);
    }
}
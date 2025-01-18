package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job3_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text user = new Text();
    private Text recommendation = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        if (line.length != 2) return;

        String[] users = line[0].split(",");
        String userA = users[0];
        String userB = users[1];
        String commonFriends = line[1];

        // Output both directions (userA recommends userB, and vice versa)
        user.set(userA);
        recommendation.set(userB + "," + commonFriends);
        context.write(user, recommendation);

        user.set(userB);
        recommendation.set(userA + "," + commonFriends);
        context.write(user, recommendation);
    }
}
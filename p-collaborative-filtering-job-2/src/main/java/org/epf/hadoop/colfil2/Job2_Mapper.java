package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2_Mapper extends Mapper<Object, Text, UserPair, Text> {

    private final Text connectionType = new Text();
    private final UserPair userPair = new UserPair();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;

        String user = parts[0];
        String[] friends = parts[1].split(",");

        // Emit pairs for common relationships
        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                userPair.set(friends[i], friends[j]);
                connectionType.set("1");
                context.write(userPair, connectionType);
            }
        }

        // Emit pairs for direct relationships
        for (String friend : friends) {
            userPair.set(user, friend);
            connectionType.set("0");
            context.write(userPair, connectionType);
        }
    }
}
package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> relationships = new ArrayList<>();
        for (Text value : values) {
            relationships.add(value.toString());
        }

        // Joindre les relations en une liste séparée par des virgules
        String result = String.join(",", relationships);
        context.write(key, new Text(result));
    }
}
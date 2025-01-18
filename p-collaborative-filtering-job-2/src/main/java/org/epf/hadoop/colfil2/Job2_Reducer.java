package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job2_Reducer extends Reducer<UserPair, Text, UserPair, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int commonCount = 0;
        boolean directRelation = false;

        for (Text value : values) {
            if (value.toString().equals("0")) {
                directRelation = true;
            } else if (value.toString().equals("1")) {
                commonCount++;
            }
        }

        // Emit only if there is no direct relationship
        if (!directRelation && commonCount > 0) {
            result.set(String.valueOf(commonCount));
            context.write(key, result);
        }
    }
}
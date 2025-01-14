package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RelationshipPartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        // Partitionner selon le hash de la clé
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
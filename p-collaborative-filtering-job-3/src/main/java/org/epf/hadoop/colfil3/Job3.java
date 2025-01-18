import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.epf.hadoop.colfil3.Job3_Mapper;
import org.epf.hadoop.colfil3.Job3_Reducer;

public class Job3 extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // Création du Job
        Job job = Job.getInstance(getConf(), "RecommendationJob");
        job.setJarByClass(Job3.class);

        // Configuration des Mapper & Reducer
        job.setMapperClass(Job3_Mapper.class);
        job.setReducerClass(Job3_Reducer.class);

        // Définir les types des clés et valeurs en sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Définir les chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exécution du job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Lancer le job en passant les arguments
        int exitCode = ToolRunner.run(new Job3(), args);
        System.exit(exitCode);
    }
}
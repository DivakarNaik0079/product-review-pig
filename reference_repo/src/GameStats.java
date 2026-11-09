import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GameStats {
    public static class StatMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] cols = value.toString().split("\t");
            // Check for valid row and skip header
            if (cols.length > 9 && !cols[0].equals("marketplace")) {
                // Key: Product ID (col 3), Value: star_rating (7), helpful_votes (8), total_votes (9)
                context.write(new Text(cols[3]), new Text(cols[7] + "," + cols[8] + "," + cols[9]));
            }
        }
    }
    public static class StatReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float maxRate = 0;
            int totalHelpful = 0, totalVotes = 0;
            for (Text val : values) {
                String[] metrics = val.toString().split(",");
                try {
                    maxRate = Math.max(maxRate, Float.parseFloat(metrics[0]));
                    totalHelpful += Integer.parseInt(metrics[1]);
                    totalVotes += Integer.parseInt(metrics[2]);
                } catch (NumberFormatException e) { continue; }
            }
            context.write(key, new Text("MaxRate: " + maxRate + "\tHelpfulVotes: " + totalHelpful + "\tTotalVotes: " + totalVotes));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Game Stats");
        job.setJarByClass(GameStats.class);
        job.setMapperClass(StatMapper.class);
        job.setReducerClass(StatReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopReviewer {
    public static class ReviewerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split("\t");
            if (columns.length > 1 && !columns[0].equals("marketplace")) {
                context.write(new Text(columns[1]), one); // Key: Customer ID
            }
        }
    }
    public static class TopReviewerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private String bestReviewer = "";
        private int maxReviews = 0;
        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int sum = 0;
            for (IntWritable val : values) { sum += val.get(); }
            if (sum > maxReviews) {
                maxReviews = sum;
                bestReviewer = key.toString();
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Best Reviewer ID: " + bestReviewer), new IntWritable(maxReviews));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Reviewer");
        job.setJarByClass(TopReviewer.class);
        job.setMapperClass(ReviewerMapper.class);
        job.setReducerClass(TopReviewerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

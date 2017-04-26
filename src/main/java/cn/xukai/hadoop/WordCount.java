package cn.xukai.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by xukai on 2017/3/9.
 */
public class WordCount extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        try {
            Configuration conf = getConf();
            conf.set("mapreduce.job.jar", "D:\\workspace\\hadoopDemo\\target\\hadoop-1.0-SNAPSHOT.jar");
            conf.set("mapreduce.framework.name", "yarn");
//            conf.set("yarn.resourcemanager.hostname", "10.20.10.100");
            conf.set("mapreduce.app-submission.cross-platform", "true");

            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCount.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.setMapperClass(WcMapper.class);
            job.setReducerClass(WcReducer.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, "hdfs://ns1/myid");
            FileOutputFormat.setOutputPath(job, new Path("hdfs://ns1/out"));

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
    public static class WcMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String mVal = value.toString();
            context.write(new Text(mVal), new LongWritable(1));
        }
    }
    public static class WcReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable lVal : values){
                sum += lVal.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordCount(), args);
    }
}

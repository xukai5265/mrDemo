package cn.xukai.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by xukai on 2017/4/14.
 */
public class Wallet extends Configured implements Tool{
    public static class MapBus extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void map(LongWritable key, Text date,
                        OutputCollector<Text, LongWritable> output,
                        Reporter reporter) throws IOException {
            //2013-01-11,-200
            String line = date.toString();
            if(line.contains(",")){
                String[] tmp = line.split(",");
                String month = tmp[0].substring(5, 7);
                int money = Integer.valueOf(tmp[1]).intValue();
                output.collect(new Text(month), new LongWritable(money));
            }
        }
    }
    public static class ReduceBus extends MapReduceBase
            implements Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text month, Iterator<LongWritable> money,
                           OutputCollector<Text, LongWritable> output, Reporter reporter)
                throws IOException {
            int total_money = 0;
            while(money.hasNext()){
                total_money += money.next().get();
            }
            output.collect(month, new LongWritable(total_money));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("param error!");
            return -1;
        }
        JobConf jobConf = new JobConf(Wallet.class);
        jobConf.setJobName("My Wallet");

        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
        jobConf.setMapperClass(MapBus.class);
        jobConf.setReducerClass(ReduceBus.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);

        try{
            JobClient.runJob(jobConf);
        }catch(Exception e){
            e.printStackTrace();
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ETL(), args);
    }
}

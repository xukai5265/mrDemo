package cn.xukai.hadoop.secondOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 本分参照： http://www.cnblogs.com/codeOfLife/p/5568786.html
 *  理解排序和分组： http://www.cnblogs.com/edisonchou/p/4299085.html
 * Created by xukai on 2017/4/26.
 * 输入文件 sort.txt 内容为
 *   40 20
     40 10
     40 30
     40 5
     30 30
     30 20
     30 10
     30 40
     50 20
     50 50
     50 10
     50 60
 输出文件的内容（从小到大排序）如下
     30 10
     30 20
     30 30
     30 40
     --------
     40 5
     40 10
     40 20
     40 30
     --------
     50 10
     50 20
     50 50
     50 60

 实现原理
    1. 将文本中的两个字段封装为一个对象（IntPair）中的两个属性。该对象实现 WritableComparable 接口并重写其方法
    2. map 阶段负责将 封装 这样的输出格式 IntPair,value
    3.
 */
public class SecondarySort extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"SecondarySort");
        job.setJarByClass(SecondarySort.class);
        // 设置map 函数
        job.setMapperClass(SecondarySort.Map.class);
        // 分区函数
        job.setPartitionerClass(FirstPartitioner.class);
        // 设置reduce 函数
        job.setReducerClass(SecondarySort.Reduce.class);

        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        //设置map输出的KEY类型
        job.setMapOutputKeyClass(IntPair.class);
        //设置map输出的VALUE类型
        job.setMapOutputValueClass(IntWritable.class);

        // reduce输出key类型
        job.setOutputKeyClass(Text.class);
        // reduce输出value类型
        job.setOutputValueClass(IntWritable.class);

        //设置输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 输入格式
        job.setInputFormatClass(TextInputFormat.class);
        // 输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        // 这是提交任务的方法，方法内部会调用job.submit()方法。
        // 如果不设置，程序是无法提交到yarn上的。
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),new SecondarySort(),args);
        System.exit(res);
    }

    public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map is start....");
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            int left = 0;
            int right = 0;
            if (tokenizer.hasMoreTokens()) {
                String tmp1 = tokenizer.nextToken();
                System.out.println("tmp1 = "+tmp1);
                left = Integer.parseInt(tmp1);
                if (tokenizer.hasMoreTokens()){
                    String tmp2 = tokenizer.nextToken();
                    System.out.println("tmp2 = "+tmp2);
                    right = Integer.parseInt(tmp2);
                }
                context.write(new IntPair(left, right), new IntWritable(right));
            }
            System.out.println("map is over....");
        }
    }

    /*
       是key的第一次比较，完成对所有key的排序。
     * 自定义分区函数类FirstPartitioner，根据 IntPair中的first实现分区
     *
     */
    public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {
        @Override
        public int getPartition(IntPair key, IntWritable value,int numPartitions){
            System.out.println("FirstPartitioner is start...");
            System.out.println("first : "+key.getFirst());
            System.out.println("numPartitions="+numPartitions);
            int res = Math.abs(key.getFirst() * 127) % numPartitions;
            System.out.println("FirstPartitioner is over...");
            return res;
        }
    }

    /*
       是key的第二次比较，对所有的key进行排序。
     * 自定义GroupingComparator类，实现分区内的数据分组
     */
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator(){
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            System.out.println("GroupingComparator is start ...");
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int l = ip1.getFirst();
            int r = ip2.getFirst();
            int res = l == r ? 0 : (l < r ? -1 : 1);
            System.out.println("l="+l + "r="+r + "res="+ res );
            System.out.println("GroupingComparator is over ...");
            return res;
        }
    }

    public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable> {

        public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("reduce is start ....");
            for (IntWritable val : values) {
                System.out.println("key= "+ key.getFirst() + " ---"+val);
                context.write(new Text(Integer.toString(key.getFirst())), val);
            }
            System.out.println("reduce is over ....");
        }
    }
}

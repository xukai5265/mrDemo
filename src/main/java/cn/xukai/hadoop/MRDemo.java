package cn.xukai.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by xukai on 2017/4/26.
 * 题目：读取文件类型如下
         sb4tF0D0 yH12ZA30gq 296.289
         oHuCS oHuCS 333.962
         oHuCS yH12ZA30gq 14.056
         sb4tF0D0 oHuCS 522.122
         oHuCS yH12ZA30gq 409.904
         sb4tF0D0 yH12ZA30gq 590.815
         sb4tF0D0 oHuCS 239.093
         sb4tF0D0 yH12ZA30gq 284.091
         sb4tF0D0 yH12ZA30gq 28.463
         sb4tF0D0 sb4tF0D0 38.819
 * 每行有空格隔开的三个字符串：<a> <b> <length>；最后一个字符串可以转化成float型。表示a通话给b，用了length的时长。
 * 问题：统计<a b>对( 与<b a>对不同）的通话次数，平均通话时间。
 *  类型如：<a> <b> <times> <avg_length>。输出文件格式如下：
         oHuCS oHuCS 1 333.962
         oHuCS yH12ZA30gq    2 211.980
         sb4tF0D0 oHuCS  2 380.607
         sb4tF0D0 sb4tF0D0   1 38.819
         sb4tF0D0 yH12ZA30gq 4 299.915

    运行：
        step1 先上传计算文本
        step2 确保输出目录不存在
        step3 执行命令提交程序：
            hadoop jar hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar /data/mrdemo/dd.txt /data/mrdemo-res
        step4 执行完毕查看log
            yarn logs -applicationId application_1493188771144_0004 > aaa.log

 */
public class MRDemo extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"mrDemo");
        job.setJarByClass(MRDemo.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 这是提交任务的方法，方法内部会调用job.submit()方法。
        // 如果不设置，程序是无法提交到yarn上的。
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),new MRDemo(), args);
        System.out.println(res);
        System.exit(res);
    }

    /**
     * 在Master和Worker之间传递DoubleWritable数组需要修改数据类型；构造新类DoubleArrayWritable 为后面的job做铺垫。
     */
    public static class DoubleArrayWritable extends ArrayWritable {
        public DoubleArrayWritable() {
            super(DoubleWritable.class);
        }
        public DoubleArrayWritable(DoubleWritable[] dw) {
            super(DoubleWritable.class);
            set(dw);
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleArrayWritable> {
        //使用新建的DoubleArrayWritable类型作为输入输出类型
        private final static DoubleWritable one = new DoubleWritable(1.0);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("mapper is start ...");
            System.out.println("输入Text value:"+value.toString());
            //step1 按照空格切字符串
            String[] in_value = value.toString().split(" ");
            //step2 对输入文本进行格式检查
            if(in_value.length != 3) return;
            //step3 数据处理
            try{
                Text out_key = new Text();
                //key值是字符串对 我的处理时将字符串对合并成一个字符串中间用$隔开
                out_key.set(in_value[0]+"$"+in_value[1]);
                DoubleWritable[] tmp = new DoubleWritable[2];
                DoubleArrayWritable out_value = new DoubleArrayWritable();
                //次数
                tmp[0] = one;
                //通话时长
                tmp[1] = new DoubleWritable(Double.valueOf(in_value[2]));
                //这里注意新建DoubleArrayWritable的赋值方案
                out_value.set(tmp);
                System.out.println("xukai:"+out_key.toString());
            //step4 发送给reduce
                context.write(out_key, out_value);
            }catch (Exception e){
                System.out.println("map error line");
            }
            System.out.println("mapper is over ...");
        }
    }

    /**
     * 目的减轻map 端的数据量，将自身map 的数据进行计算。减轻reduce 获取map 结果的网络流量。
     */
    public static class IntSumCombiner extends Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {
        private DoubleArrayWritable out_value = new DoubleArrayWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("combiner is start...");
            System.out.println("key = " + key.toString());
            double times_sum = 0.0,sum = 0.0;
            for (DoubleArrayWritable val : values) {
                System.out.println("通话次数 = "+ val.get()[0].toString());
                System.out.println("通话时长 = "+ val.get()[1].toString());

                times_sum += Double.valueOf(val.get()[0].toString());//这里注意DoubleArrayWritable的读取方案
                sum       += Double.valueOf(val.get()[1].toString());
            }
            System.out.println("通话次数总和 = " + times_sum);
            System.out.println("通话时长总和 = " + sum);
            DoubleWritable[] tmp = new DoubleWritable[2];
            tmp[0] = new DoubleWritable(times_sum);
            tmp[1] = new DoubleWritable(sum);
            out_value.set(tmp);
            context.write(key, out_value);
            System.out.println("combiner is over...");
        }
    }

    public static class IntSumReducer extends Reducer<Text, DoubleArrayWritable, Text, Text> {
        private Text result_key = new Text();
        private Text result_value = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("reduce is start ...");
            double times_sum = 0.0,sum = 0.0;
            for (DoubleArrayWritable val : values) {
                times_sum += Double.valueOf(val.get()[0].toString());
                sum       += Double.valueOf(val.get()[1].toString());
            }
            sum /= times_sum;
            System.out.println(key.toString() + "--- 次数 = " + times_sum);
            System.out.println(key.toString() + "--- 平均通话时长 = "+ sum);
            DecimalFormat decimalFormat1 = new DecimalFormat("");
            DecimalFormat decimalFormat2 = new DecimalFormat(".000"); //float型数据后保留3位小数
            String times_sum_string = decimalFormat1.format(times_sum);
            String sum_string = decimalFormat2.format(sum);
            // generate result key
            String out_key = new String(key.getBytes(),0,key.getLength(),"GBK").replace("$"," ");
            System.out.println("out_key = "+out_key);
            result_key.set(out_key);
            // generate result value
            result_value.set( times_sum_string+" "+sum_string );
            context.write(result_key, result_value);
            System.out.println("reduce is over ...");
        }
    }

}

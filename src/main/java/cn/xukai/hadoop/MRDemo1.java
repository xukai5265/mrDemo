package cn.xukai.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

/**
 * Created by xukai on 2017/4/26.
 */
public class MRDemo1 extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(),"mrDemo");
        job.setJarByClass(MRDemo1.class);
        job.setMapperClass(MRDemo1.MProcess.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Map.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 这是提交任务的方法，方法内部会调用job.submit()方法。
        // 如果不设置，程序是无法提交到yarn上的。
        job.waitForCompletion(true);
        return 0;
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

    public static class MProcess extends Mapper<Object,Text,Text,MRDemo1.DoubleArrayWritable>{
        //使用新建的DoubleArrayWritable类型作为输入输出类型
        private final static DoubleWritable one = new DoubleWritable(1.0);
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("map is start ...");
            System.out.println("输入： "+value.toString());
            String [] values = value.toString().split(" ");
            if(values.length!=3)return;
            String key_out = values[0]+"$"+values[1];
            System.out.println("key_out = " + key_out);
            DoubleArrayWritable daw = new DoubleArrayWritable();
            DoubleWritable[] tmp = new DoubleWritable[2];
            tmp[0] = one;
            tmp[1] = new DoubleWritable(Double.valueOf(values[2]));
            daw.set(tmp);
            context.write(new Text(key_out),daw);
            System.out.println("map is over ...");
        }
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),new MRDemo1(),args);
        System.exit(res);
    }
}

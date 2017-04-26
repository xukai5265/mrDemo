package cn.xukai.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.StringTokenizer;

/**
 * Created by xukai on 2017/4/11.
 */
public class ETL extends Configured implements Tool {
    public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH); //原时间格式
    public static final SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyy-MM-dd");//现时间格式
    private Date parseDateFormat(String string) {         //转换时间格式
        Date parse = null;
        try {
            parse = FORMAT.parse(string);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return parse;
    }
    public String[] parse(String line) {
        String time = parseTime(line);   //时间
        String url = parseURL(line);     //url
        String status = parseStatus(line); //状态
        return new String[] { time,status,url };
    }
    private String parseStatus(String line) {     //状态
        final String trim = line.substring(line.lastIndexOf("\"") + 1)
                .trim();
        String status = trim.split(" ")[0];
        return status;
    }
    private String parseURL(String line) {       //url
        final int first = line.indexOf("\"");
        final int last = line.lastIndexOf("\"");
        String url = line.substring(first + 1, last);
        return url;
    }
    private String parseTime(String line) {    //时间
        final int first = line.indexOf("[");
        final int last = line.indexOf("+0800]");
        String time = line.substring(first + 1, last).trim();
        Date date = parseDateFormat(time);
        return dateformat1.format(date);
    }

    public static class ETLMap extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //将输入的纯文本文件的数据转化成String
            Text outputValue = new Text();
            String line = value.toString();
            ETL aa = new ETL();
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
            //分别对每一行进行处理
            while (tokenizerArticle.hasMoreElements()) {
                //每行转换成一个字符串类型
                String stra=tokenizerArticle.nextToken().toString();
                String [] Newstr=aa.parse(stra);
                if (Newstr[2].startsWith("GET /")) { //过滤开头字符串
                    Newstr[2] = Newstr[2].substring("GET /".length());
                }
                else if (Newstr[2].startsWith("POST /")) {
                    Newstr[2] = Newstr[2].substring("POST /".length());
                }
                if (Newstr[2].endsWith(" HTTP/1.1")) { //过滤结尾字符串
                    Newstr[2] = Newstr[2].substring(0, Newstr[2].length()
                            - " HTTP/1.1".length());
                }
                String[] words = Newstr[2].split("/");
                if(words.length==4 && Newstr[1].equals("404")==false){
                    outputValue.set(Newstr[0]+"\t"+words[1]+"\t"+words[2]+"\t"+words[3]);
                    context.write(outputValue,new IntWritable(1));
                }
            }
        }
    }
    public static class ETLReduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        // 实现reduce函数
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                sum += iterator.next().get();
            }
            context.write(key, new IntWritable(sum));
        }
    }



    @Override
    public int run(String[] strings) throws Exception {
        try {
            Configuration conf = getConf();
//            conf.set("mapreduce.job.jar", "/application/runjar/hadoop-1.0-SNAPSHOT.jar");
            conf.set("mapreduce.job.jar", "D:\\workspace\\hadoopDemo\\target\\hadoop-1.0-SNAPSHOT.jar");
            conf.set("mapreduce.framework.name", "yarn");
//            conf.set("yarn.resourcemanager.hostname", "10.20.10.100");
            conf.set("mapreduce.app-submission.cross-platform", "true");

            Job job = Job.getInstance(conf,"etl");
            job.setJarByClass(ETL.class);

            //设置输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //设置map 、combiner 、 reduce 处理类
            job.setMapperClass(ETL.ETLMap.class);
            job.setCombinerClass(ETL.ETLReduce.class);
            job.setReducerClass(ETL.ETLReduce.class);

            // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
            job.setInputFormatClass(TextInputFormat.class);
            // 提供一个RecordWriter的实现，负责数据输出
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.setInputPaths(job, "hdfs://192.168.137.140:9000/data/produce.txt");
            FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.137.140:9000/data/out"));

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ETL(), args);
    }
}

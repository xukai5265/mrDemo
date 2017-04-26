package cn.xukai.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * Created by xukai on 2017/4/26.
 * 打印出hadoop 上的配置参数
 */
public class ToolRunnerDemo extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        return 0;
    }
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ToolRunnerDemo(), args);
        System.exit(exitCode);
    }
}

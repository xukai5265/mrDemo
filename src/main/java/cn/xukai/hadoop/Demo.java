package cn.xukai.hadoop;
/**
 * Created by xukai on 2017/4/11.
 */
public class Demo {
    public native boolean g729topcma(final String p0, final String p1); //java 代码中声明 native 方法

    static {
        System.load("D:\\工作\\2017年\\04月\\11日\\libHME_Codec_G729.so");  //以绝对路径加载so文件
    }



    public static void main(String[] args) {
        Demo demo = new Demo();
        String input= "D:\\工作\\2017年\\04月\\11日\\录音";
        String output="D:\\工作\\2017年\\04月\\11日\\res";
        demo.g729topcma(input,output);
    }
}

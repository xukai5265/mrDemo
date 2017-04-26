package cn.xukai.hadoop;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xukai on 2017/3/9.
 */
public class RegexMapper {
    @Test
    public void test1(){
        String str = "111.196.199.171 - - [09/Mar/2017:09:33:14 +0800] \"GET /qa/images/icons-sdaa897434a-a520797944.png HTTP/1.1\" 304 0 \"http://qa.lingban.cn/qa/css/project-3157dbbc0f.css\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36\" \"-\"";
        String regex = "\\d{1,3}.\\d{1,3}.\\d{1,3}.\\d{1,3}.*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if(matcher.find()){
            for(int i =1; i<=matcher.groupCount();i++){
                System.out.println(matcher.group(i));
            }
        }


    }
}

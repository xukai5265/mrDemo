package cn.xukai.hadoop.secondOrder;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xukai on 2017/4/26.
 * 将示例数据中的key/value封装成一个整体作为Key，同时实现 WritableComparable接口并重写其方法
 */
public class IntPair implements WritableComparable<IntPair>{

    private int first;
    private int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(IntPair o) {
        if (first != o.first){
            return first < o.first ? -1 : 1;
        }else if (second != o.second){
            return second < o.second ? -1 : 1;
        }else{
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object right) {
        if (right == null)
            return false;
        if (this == right)
            return true;
        if (right instanceof IntPair){
            IntPair r = (IntPair) right;
            return r.first == first && r.second == second;
        }else{
            return false;
        }
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }
}

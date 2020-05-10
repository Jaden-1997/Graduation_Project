package com.mapred;

import com.beans.Patient;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class XGReducer extends Reducer<Patient, Text, Patient,Text> {


    @Override
    /*
    *   每一个key 根据compareTo来判断是否是同一个key ,遍历相同的key相同就会把 value 放到 Iterable vaLue里面去,然后调用reduce方法
    *   key都得currentKey,还是不是很理解
    * */
    protected void reduce(Patient key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Text redvalue = new Text();
        String isxg = "";

        for (Text reduce_value:values) {

            isxg +=  key.getName() +":"+key.getFlag()+":" + reduce_value.toString();
        }
        redvalue.set(isxg);
        context.write(key,redvalue);

    }
}

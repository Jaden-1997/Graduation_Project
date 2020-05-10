package com.mapred;

import com.beans.Patient;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class XGMapper extends Mapper<LongWritable, Text, Patient, Text> {

    Text map_value = new Text();
    Patient p = new Patient();
    Patient p_age = new Patient();
    Patient p_gender = new Patient();
    Patient p_tb = new Patient();
    Patient p_db = new Patient();
    Patient p_alkphos = new Patient();
    Patient p_sgpt = new Patient();
    Patient p_sgot = new Patient();
    Patient p_tp = new Patient();
    Patient p_alb = new Patient();
    Patient p_a_g = new Patient();

    ArrayList<Patient> arrayList = new ArrayList<Patient>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取一行
        String line = value.toString();
        String[] atrr = line.split(",");

        String name = atrr[0];
        String age = atrr[1];
        String gender = atrr[2];
        String tb = atrr[3];
        String db = atrr[4];
        String alkphos = atrr[5];
        String sgpt = atrr[6];
        String sgot = atrr[7];
        String tp = atrr[8];
        String alb = atrr[9];
        String a_g = atrr[10];
        String is_class = atrr[11];

        p.setName(name);
        p.setAge(age);
        p.setGender(gender);
        p.setTb(tb);
        p.setDb(db);
        p.setAlkphos(alkphos);
        p.setSgpt(sgpt);
        p.setSgot(sgot);
        p.setTp(tp);
        p.setAlb(alb);
        p.setA_g(a_g);
        p.setIs_class(is_class);

        //输出原本P
        p.setFlag();
        map_value.set(p.getIs_class());
        context.write(p,map_value);

        //输出p-age
        p_age = p;
        p_age.setAge("-");
        p_age.setFlag();
        context.write(p_age,new Text(p_age.getIs_class()));

        //输出p-gender
        p_gender = p;
        p_gender.setGender("-");
        p_gender.setFlag();
        context.write(p_gender,new Text(p_gender.getIs_class()));

        //输出p-tb
        p_tb = p;
        p_tb.setTb("-");
        p_tb.setFlag();
        context.write(p_tb,new Text(p_tb.getIs_class()));

        //输出p-db
        p_db = p;
        p_db.setDb("-");
        p_db.setFlag();
        context.write(p_db,new Text(p_db.getIs_class()));

        //输出p-alkphos
        p_alkphos = p;
        p_alkphos.setAlkphos("-");
        p_alkphos.setFlag();
        context.write(p_alkphos,new Text(p_alkphos.getIs_class()));

        //输出p-sgpt
        p_sgpt = p;
        p_sgpt.setSgpt("-");
        p_sgpt.setFlag();
        context.write(p_sgpt,new Text(p_sgpt.getIs_class()));

        //输出p-sgot
        p_sgot = p;
        p_sgot.setSgot("-");
        p_sgot.setFlag();
        context.write(p_sgot,new Text(p_sgot.getIs_class()));

        //输出p-tp
        p_tp = p;
        p_tp.setTp("-");
        p_tp.setFlag();
        context.write(p_tp,new Text(p_tp.getIs_class()));

        //输出p-alb
        p_alb = p;
        p_alb.setAlb("-");
        p_alb.setFlag();
        context.write(p_alb,new Text(p_alb.getIs_class()));

        //输出p-a_g
        p_a_g = p;
        p_a_g.setA_g("-");
        p_a_g.setFlag();
        context.write(p_a_g,new Text(p_a_g.getIs_class()));

    }

}

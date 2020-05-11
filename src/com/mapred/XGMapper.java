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

        //初始化对象
        Copy_Patient(p,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_age,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_gender,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_tb,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_db,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_alkphos,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_sgpt,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_sgot,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_tp,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_alb,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        Copy_Patient(p_a_g,name,age,gender,tb,db,alkphos,sgpt,sgot,tp,alb,a_g,is_class);
        //输出原本P
        map_value.set(p.getIs_class());
        context.write(p,map_value);

        //输出p_age
        p_age.setAge("-");
        Write_Patient(p_age,context);

        //输出p_gender
        p_gender.setGender("-");
        Write_Patient(p_gender,context);

        //输出p_tb
        p_tb.setTb("-");
        Write_Patient(p_tb,context);

        //输出p_db
        p_db.setDb("-");
        Write_Patient(p_db,context);

        //输出p_alkphos
        p_alkphos.setAlkphos("-");
        Write_Patient(p_alkphos,context);

        //输出p_sgpt
        p_sgpt.setSgpt("-");
        Write_Patient(p_sgpt,context);

        //输出p_sgot
        p_sgot.setSgot("-");
        Write_Patient(p_sgot,context);

        //输出p_tp
        p_tp.setTp("-");
        Write_Patient(p_tp,context);

        //输出p_alb
        p_alb.setAlb("-");
        Write_Patient(p_alb,context);

        //输出p_a_g
        p_a_g.setA_g("-");
        Write_Patient(p_a_g,context);
    }

    public void Copy_Patient(Patient p , String name ,String age ,String gender ,String tb,String db,String alkphos,String sgpt,String sgot,String tp,String alb ,String a_g,String is_class){

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
    }

    public void Write_Patient(Patient p,Context context) throws IOException, InterruptedException {

        p.setFlag();
        map_value.set(p.getIs_class());
        context.write(p,map_value);

    }
}

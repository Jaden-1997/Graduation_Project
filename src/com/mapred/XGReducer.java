package com.mapred;

import com.beans.Patient;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class XGReducer extends Reducer<Patient, Text, Text,Text> {

    HashSet<String> p_set = new HashSet<String>();
    HashSet<String> p_age_set = new HashSet<String>();
    HashSet<String> p_gender_set = new HashSet<String>();
    HashSet<String> p_tb_set = new HashSet<String>();
    HashSet<String> p_db_set = new HashSet<String>();
    HashSet<String> p_alkphos_set = new HashSet<String>();
    HashSet<String> p_sgpt_set = new HashSet<String>();
    HashSet<String> p_sgot_set = new HashSet<String>();
    HashSet<String> p_tp_set = new HashSet<String>();
    HashSet<String> p_alb_set = new HashSet<String>();
    HashSet<String> p_a_g_set = new HashSet<String>();

    //HashSet<HashSet> unSortSet = new HashSet<HashSet>();

    //对set进行排序
    Map<Integer,HashSet> sortMap = new TreeMap<Integer,HashSet>(new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o2-o1;
        }
    });
    @Override
    /*
    *   每一个key 根据compareTo来判断是否是同一个key ,遍历相同的key相同就会把 value 放到 Iterable vaLue里面去,然后调用reduce方法
    *   key都得currentKey,还是不是很理解
    * */
    protected void reduce(Patient key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //都是局部变量
        Text redvalue = new Text();
        String is_class = "";
        String info = "";
        String names = "";

        for (Text reduce_value:values) {

            /*检查所有等价类的信息*/
            info += "{"+key.getName()+":"+reduce_value.toString()+":"+key.getFlag()+"}";
            //x_name += key.getName();
            is_class += reduce_value.toString();
            names += key.getName()+",";
        }
//        redvalue.set(is_class);
//        context.write(key,redvalue);
        if(!Class_Equals(is_class)){


        }else {

            //开始分类，把属性约简后的 和 原本集合正域 分别放到不同的set里 （set是否只用存储name就够了）
            if(key.getFlag().charAt(0)=='-') {          //charAt(0) 去掉年龄

                NameArrtoSet(names,p_age_set);
//                redvalue.set("reduceAge:"+is_class+"["+info+"]"+String.valueOf(p_age_set.size()));
//                context.write(key, redvalue);


            }else if(key.getFlag().charAt(1)=='-'){     //charAt(1) 去掉性别
                //redvalue.set(is_class+"["+info+"]");
                //context.write(key, redvalue);
                NameArrtoSet(names,p_gender_set);

            }else if(!(key.getFlag().contains("-"))){   //原始正域

                //这个时候的Key是最后一个最后一个对象（key相同的同一组中）
                NameArrtoSet(names,p_set);
                //redvalue.set(is_class+"["+info+"]"+String.valueOf(p_set.size()));
                //context.write(key, redvalue);
            }else if(key.getFlag().charAt(2)=='-'){     //charAt(2)去掉tb
                NameArrtoSet(names,p_tb_set);
            }else if(key.getFlag().charAt(3)=='-'){     //charAt(3)去掉db
                NameArrtoSet(names,p_db_set);
            }else if(key.getFlag().charAt(4)=='-'){     //charAt(4)去掉alkphos
                NameArrtoSet(names,p_alkphos_set);
            }else if(key.getFlag().charAt(5)=='-'){     //charAt(5)去掉sgpt
                NameArrtoSet(names,p_sgpt_set);
            }else if(key.getFlag().charAt(6)=='-'){     //charAt(6)去掉sgot
                NameArrtoSet(names,p_sgot_set);
            }else if(key.getFlag().charAt(7)=='-'){     //charAt(7)去掉tp
                NameArrtoSet(names,p_tp_set);
            }else if(key.getFlag().charAt(8)=='-'){     //charAt(8)去掉alb
                NameArrtoSet(names,p_alb_set);
            }else if(key.getFlag().charAt(9)=='-'){     //charAt(2)去掉a_g
                NameArrtoSet(names,p_a_g_set);
            }

        }//if(is_class)

    }//reduce

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        /*
        *   去掉属性和没去掉属性的正域
        * */
        context.write(new Text("p_set:"),new Text(String.valueOf(p_set.size())));
        context.write(new Text("p_age_set:"),new Text(String.valueOf(p_age_set.size())));
        context.write(new Text("p_gender_set:"),new Text(String.valueOf(p_gender_set.size())));
        context.write(new Text("p_tb_set:"),new Text(String.valueOf(p_tb_set.size())));
        context.write(new Text("p_db_set:"),new Text(String.valueOf(p_db_set.size())));
        context.write(new Text("p_alkphos_set:"),new Text(String.valueOf(p_alkphos_set.size())));
        context.write(new Text("p_sgpt_set:"),new Text(String.valueOf(p_sgpt_set.size())));
        context.write(new Text("p_sgot_set:"),new Text(String.valueOf(p_sgot_set.size())));
        context.write(new Text("p_tp_set:"),new Text(String.valueOf(p_tp_set.size())));
        context.write(new Text("p_alb_set:"),new Text(String.valueOf(p_alb_set.size())));
        context.write(new Text("p_a_g_set:"),new Text(String.valueOf(p_a_g_set.size())));

        //把set对象放进set_arr
        sortMap.put(p_age_set.size(),p_age_set);
        //sortMap.put(p_gender_set.size(),p_gender_set);
        sortMap.put(p_tb_set.size(),p_tb_set);
        sortMap.put(p_db_set.size(),p_db_set);
        //sortMap.put(p_alkphos_set.size(),p_alkphos_set);
        sortMap.put(p_sgpt_set.size(),p_sgpt_set);
        sortMap.put(p_sgot_set.size(),p_sgot_set);
        sortMap.put(p_tp_set.size(),p_tp_set);
        sortMap.put(p_alb_set.size(),p_alb_set);
        sortMap.put(p_a_g_set.size(),p_a_g_set);

        HashSet<String> set_flag = new HashSet<String>();
        for(Map.Entry<Integer,HashSet> entry:sortMap.entrySet()){

            if(CompareSet(p_set,set_flag)){
                break;
            }else {
                HashSet set = entry.getValue();
                set_flag = CombineSet(set_flag,set);
                context.write(new Text("set_size:"),new Text(String.valueOf(set_flag.size())));
                context.write(new Text("set_flag:"),new Text(String.valueOf(CompareSet(p_set,set_flag))));
            }
        }

        /*
        //sgpt db 相加 然后对比set
        HashSet<String> set_sgpt_db = CombineSet(p_sgpt_set,p_db_set);
        context.write(new Text("set_sgpt_db:"),new Text(String.valueOf(set_sgpt_db.size())));
        context.write(new Text("sgpt_db:"),new Text(String.valueOf(CompareSet(p_set,set_sgpt_db))));

        //这里又加上了alb 集合已经大于 原来的正域
        HashSet<String> set_sgpt_db_alb = CombineSet(set_sgpt_db,p_alb_set);
        context.write(new Text("set_sgpt_db_alb:"),new Text(String.valueOf(set_sgpt_db_alb.size())));
        context.write(new Text("sgpt_db_alb:"),new Text(String.valueOf(CompareSet(p_set,set_sgpt_db_alb))));

        //加上性别
        HashSet<String> set_sgpt_db_alb_gender = CombineSet(set_sgpt_db_alb,p_gender_set);
        context.write(new Text("set_sgpt_db_alb_gender:"),new Text(String.valueOf(set_sgpt_db_alb_gender.size())));
        context.write(new Text("sgpt_db_alb_gender:"),new Text(String.valueOf(CompareSet(set_sgpt_db_alb_gender,set_sgpt_db_alb))));

        //加上年龄
        HashSet<String> set_sgpt_db_alb_gender_age = CombineSet(set_sgpt_db_alb_gender,p_gender_set);
        context.write(new Text("set_sgpt_db_alb_gender_age:"),new Text(String.valueOf(set_sgpt_db_alb_gender_age.size())));
        context.write(new Text("sgpt_db_alb_gender_age:"),new Text(String.valueOf(CompareSet(set_sgpt_db_alb,set_sgpt_db_alb_gender_age))));
        */
    }

    //判断一个集合里的is_class是否相同 true 相同 false不同
    public boolean Class_Equals(String class_str){

        //获得第一个is_class
        char first = class_str.charAt(0);

        //默认都是is_class相同
        int flag = 1;
        for(int i = 0 ; i < class_str.length(); i++){

            //System.out.println(class_str.charAt(i)==first);
            if(class_str.charAt(i)!=first){
                flag = 0;
                break;
            }
        }

        //最终输出
        if(flag==1){
            return true;
        }else {
            return false;
        }
    }

    //切割 names中的patientname ,然后加入set中
    public void NameArrtoSet(String names,HashSet<String> set){

        String[] name_arr = names.split(",");
        for (String name:name_arr) {
            set.add(name);
        }
    }

    //把set1  和 set2 加入set3 生成一个集合
    public HashSet<String> CombineSet(HashSet<String> set1 , HashSet<String> set2){

        HashSet<String> set3 = new HashSet<String>();
        for (String s1:set1) {
            set3.add(s1);
        }
        for (String s2:set2) {
            set3.add(s2);
        }

        return set3;
    }

    //比较 target中的元素 都在 source_set
    public boolean CompareSet(HashSet<String> target_set,HashSet<String> source_set){

        int flag = 1;

        for (String name:target_set) {

            if(!source_set.contains(name)){
                flag = 0;
                break;
            }
        }

        if(flag == 1){
            return true;
        }else {
            return false;
        }
    }


}

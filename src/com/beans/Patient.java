package com.beans;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Patient implements WritableComparable<Patient> {

    //对象名字
    private String name;
    /*
        条件属性：age、gender、TB、DB、alkphos、sgpt、sgot、TP、ALB、A_G、
    */
    private String age;
    private String gender;
    private String tb;
    private String db;
    private String alkphos;
    private String sgpt;
    private String sgot;
    private String tp;
    private String alb;
    private String a_g;
    /*
        决策属性is_class
    */
    private String is_class;
    /*
        判断标志flag
    */
    private String flag;

    public Patient() {
    }

    public Patient(String name, String age, String gender, String tb, String db, String alkphos, String sgpt, String sgot, String tp, String alb, String a_g, String is_class) {
        this.name = name;
        this.age = age;
        this.gender = gender;
        this.tb = tb;
        this.db = db;
        this.alkphos = alkphos;
        this.sgpt = sgpt;
        this.sgot = sgot;
        this.tp = tp;
        this.alb = alb;
        this.a_g = a_g;
        this.is_class = is_class;
        this.flag=this.age+this.gender+this.tb+this.db+this.alkphos+this.sgpt+this.sgot+this.tp+this.alb+this.a_g;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getTb() {
        return tb;
    }

    public void setTb(String tb) {
        this.tb = tb;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getAlkphos() {
        return alkphos;
    }

    public void setAlkphos(String alkphos) {
        this.alkphos = alkphos;
    }

    public String getSgpt() {
        return sgpt;
    }

    public void setSgpt(String sgpt) {
        this.sgpt = sgpt;
    }

    public String getSgot() {
        return sgot;
    }

    public void setSgot(String sgot) {
        this.sgot = sgot;
    }

    public String getTp() {
        return tp;
    }

    public void setTp(String tp) {
        this.tp = tp;
    }

    public String getAlb() {
        return alb;
    }

    public void setAlb(String alb) {
        this.alb = alb;
    }

    public String getA_g() {
        return a_g;
    }

    public void setA_g(String a_g) {
        this.a_g = a_g;
    }

    public String getIs_class() {
        return is_class;
    }

    public void setIs_class(String is_class) {
        this.is_class = is_class;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag() {
        this.flag=this.age+this.gender+this.tb+this.db+this.alkphos+this.sgpt+this.sgot+this.tp+this.alb+this.a_g;
    }

    @Override
    public int compareTo(Patient o) {

        if((this.flag).equals(o.getFlag())){
            return 0;
        }else {
            return -1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(name);
        dataOutput.writeUTF(age);
        dataOutput.writeUTF(gender);
        dataOutput.writeUTF(tb);
        dataOutput.writeUTF(db);
        dataOutput.writeUTF(alkphos);
        dataOutput.writeUTF(sgpt);
        dataOutput.writeUTF(sgot);
        dataOutput.writeUTF(tp);
        dataOutput.writeUTF(alb);
        dataOutput.writeUTF(a_g);
        dataOutput.writeUTF(is_class);
        dataOutput.writeUTF(flag);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        name = dataInput.readUTF();
        age = dataInput.readUTF();
        gender = dataInput.readUTF();
        tb = dataInput.readUTF();
        db = dataInput.readUTF();
        alkphos = dataInput.readUTF();
        sgpt = dataInput.readUTF();
        sgot = dataInput.readUTF();
        tp = dataInput.readUTF();
        alb = dataInput.readUTF();
        a_g = dataInput.readUTF();
        is_class = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    public static void main(String[] args) {

        Patient p = new Patient("1","2","3","1","1","4","5","6","7","1","1","1");
        Patient new_p = new Patient();

        new_p = p;
        new_p.setAge("-");
        System.out.println((new_p.getAge()+new_p.getGender()+new_p.getSgot()).equals("-36"));

    }
}

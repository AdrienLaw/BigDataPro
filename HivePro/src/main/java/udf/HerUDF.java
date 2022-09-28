package udf;


import org.apache.hadoop.hive.ql.exec.UDF;

public class HerUDF extends UDF {
    public String evaluate(String in){
        return in.toLowerCase();
    }
    public int evaluate(int a,int b){
        return a + b;
    }
}

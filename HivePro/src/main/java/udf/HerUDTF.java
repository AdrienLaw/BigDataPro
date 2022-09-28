package udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class HerUDTF  extends GenericUDTF {
    private final transient Object[] forwardListObj = new Object[1];

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //设置列名的类型
        List<String> fieldNames = new ArrayList<>();
        //设置列名
        fieldNames.add("column_01");
        //检查器列表
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>()  ;

        //设置输出的列的值类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    // select myudtf("zookeeper,hadoop,hdfs,hive,MapReduce",",") word;
    // ("zookeeper,hadoop,hdfs,hive,MapReduce",",") 通过Object[] objects传入函数
    @Override
    public void process(Object[] objects) throws HiveException {
        //1:获取原始数据
        String args = objects[0].toString();
        //2:获取数据传入的第二个参数，此处为分隔符
        String splitKey = objects[1].toString();
        //3.将原始数据按照传入的分隔符进行切分
        String[] fields = args.split(splitKey);
        //4:遍历切分后的结果，并写出
        for (String field : fields) {
            //将每一个单词添加值对象数组
            forwardListObj[0] = field;
            //将对象数组内容写出
            forward(forwardListObj);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}

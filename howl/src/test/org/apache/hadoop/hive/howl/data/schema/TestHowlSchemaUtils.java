package org.apache.hadoop.hive.howl.data.schema;

import java.io.PrintStream;

import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import junit.framework.TestCase;

public class TestHowlSchemaUtils extends TestCase {

    public void testSimpleOperation() throws Exception{
        String typeString = "struct<name:string,studentid:int,"
            + "contact:struct<phno:string,email:string>,"
            + "currently_registered_courses:array<string>,"
            + "current_grades:map<string,string>,"
            + "phnos:array<struct<phno:string,type:string>>,blah:array<int>>";
        
        TypeInfo ti = TypeInfoUtils.getTypeInfoFromTypeString(typeString);

        HowlSchema hsch = HowlSchemaUtils.getHowlSchemaFromTypeString(typeString);
        System.out.println(ti.getTypeName());
        System.out.println(hsch.toString());
        assertEquals(ti.getTypeName(),hsch.toString());
        assertEquals(hsch.toString(),typeString);
    }

    @SuppressWarnings("unused")
    private void pretty_print(PrintStream pout, HowlSchema hsch) throws HowlException {
        pretty_print(pout,hsch,"");
    }


    private void pretty_print(PrintStream pout, HowlSchema hsch, String prefix) throws HowlException {
        int i = 0;
        for (HowlFieldSchema field : hsch.getFields()){
            pretty_print(pout,field,prefix+"."+(field.getName()==null?i:field.getName()));
            i++;
        }
    }
    private void pretty_print(PrintStream pout, HowlFieldSchema hfsch, String prefix) throws HowlException {
        
        Category tcat = hfsch.getCategory();
        if (Category.STRUCT == tcat){
            pretty_print(pout,hfsch.getStructSubSchema(),prefix);
        }else if (Category.ARRAY == tcat){
            pretty_print(pout,hfsch.getArrayElementSchema(),prefix);
        }else if (Category.MAP == tcat){
            pout.println(prefix + ".mapkey:\t" + hfsch.getMapKeyType().toString());
            pretty_print(pout,hfsch.getMapValueSchema(),prefix+".mapvalue:");
        }else{
            pout.println(prefix + "\t" + hfsch.getType().toString());
        }
    }

}

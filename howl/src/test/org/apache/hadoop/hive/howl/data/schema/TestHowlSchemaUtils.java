package org.apache.hadoop.hive.howl.data.schema;

import java.io.PrintStream;

import org.apache.hadoop.hive.howl.common.HowlException;
import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema.Category;
import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema.Type;
import org.apache.hadoop.hive.howl.data.type.HowlTypeInfo;
import org.apache.hadoop.hive.howl.data.type.HowlTypeInfoUtils;

import junit.framework.TestCase;

public class TestHowlSchemaUtils extends TestCase {

    public void testSimpleOperation() throws Exception{
        String typeString = "struct<name:string,studentid:int,"
            + "contact:struct<phno:string,email:string>,"
            + "currently_registered_courses:array<string>,"
            + "current_grades:map<string,string>,"
            + "phnos:array<struct<phno:string,type:string>>,blah:array<int>>";
        HowlTypeInfo hti = HowlTypeInfoUtils.getHowlTypeInfo(typeString);

        HowlSchema hsch = HowlSchemaUtils.getHowlSchema(typeString);
//        PrintStream p1 = new PrintStream(System.out);
//        pretty_print(p1,hti);
//        PrintStream p2 = new PrintStream(System.out);
//        pretty_print(p2,hsch);
        System.out.println(hti.getTypeString());
        System.out.println(hsch.toString());
        assertEquals(hti.getTypeString(),hsch.toString());
        assertEquals(hsch.toString(),typeString);
    }

    @SuppressWarnings("unused")
    private void pretty_print(PrintStream pout, HowlTypeInfo hti) {
        pretty_print(pout,hti,"");
    }

    @SuppressWarnings("unused")
    private void pretty_print(PrintStream pout, HowlSchema hsch) throws HowlException {
        pretty_print(pout,hsch,"");
    }

    private void pretty_print(PrintStream pout, HowlTypeInfo hti, String prefix) {
        Type t = hti.getType();
        pout.println(prefix + "===" + hti.getTypeString());
        if (Type.STRUCT == t){
            int i = 0;
            for (HowlTypeInfo field : hti.getAllStructFieldTypeInfos()){
                pretty_print(pout,field,prefix+"."+i);
                i++;
            }
        }else if (Type.ARRAY == t){
            pretty_print(pout,hti.getListElementTypeInfo(),prefix+".array:");
        }else if (Type.MAP == t){
            pretty_print(pout,hti.getMapKeyTypeInfo(),prefix+".mapkey:");
            pretty_print(pout,hti.getMapValueTypeInfo(),prefix+".mapvalue:");
        }else{
            pout.println(prefix + "\t" + t.toString());
        }
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

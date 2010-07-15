package org.apache.hadoop.hive.howl.data;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class HowlTypeInfo extends TypeInfo {

  TypeInfo baseTypeInfo = null;

  // populated if the base type is a struct
  List<HowlTypeInfo> listFields = null;

  // populated if base type is a list
  HowlTypeInfo listType = null;

  // populated if the base type is a map
  HowlTypeInfo mapKeyType = null;
  HowlTypeInfo mapValueType = null;

  @SuppressWarnings("unused")
  private HowlTypeInfo(){
    // preventing empty ctor from being callable
  }

  public HowlTypeInfo(Schema s){
    List<FieldSchema> fields = s.getFieldSchemas();
    List<String> names = new ArrayList<String>();
    List<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
    for (FieldSchema f : fields){
      names.add(f.getName());
      typeInfos.add(TypeInfoUtils.getTypeInfoFromTypeString(f.getType()));
    }
    this.baseTypeInfo = TypeInfoFactory.getStructTypeInfo(names, typeInfos);
    deepTraverseAndSetup();
  }

  HowlTypeInfo(FieldSchema fs){
    this(fs.getType());
  }

  public HowlTypeInfo(String fstype) {
    this(TypeInfoUtils.getTypeInfoFromTypeString(fstype));
  }

  public HowlTypeInfo(TypeInfo typeInfo){
    this.baseTypeInfo = typeInfo;
    deepTraverseAndSetup();
  }

  private void deepTraverseAndSetup() {
    if (baseTypeInfo.getCategory() == Category.MAP){
      mapKeyType = new HowlTypeInfo(((MapTypeInfo)baseTypeInfo).getMapKeyTypeInfo());
      mapValueType = new HowlTypeInfo(((MapTypeInfo)baseTypeInfo).getMapValueTypeInfo());
    }else if (baseTypeInfo.getCategory() == Category.LIST){
      listType = new HowlTypeInfo(((ListTypeInfo)baseTypeInfo).getListElementTypeInfo());
    }else if (baseTypeInfo.getCategory() == Category.STRUCT){
      for(TypeInfo ti : ((StructTypeInfo)baseTypeInfo).getAllStructFieldTypeInfos()){
        listFields.add(new HowlTypeInfo(ti));
      }
    }
  }

  // TODO : throw exception if null? do we want null or exception semantics?

  public HowlTypeInfo getMapKeyTypeInfo(){
    return mapKeyType;
  }

  public HowlTypeInfo getMapValueTypeInfo(){
    return mapValueType;
  }

  public HowlTypeInfo getListElementTypeInfo(){
    return listType;
  }

  public List<HowlTypeInfo> getAllStructFieldTypeInfos(){
    return listFields;
  }

  public TypeInfo getBaseTypeInfo(){
    return baseTypeInfo;
  }

  @Override
  public Category getCategory() {
    return baseTypeInfo.getCategory();
  }

  @Override
  public String getTypeName() {
    return baseTypeInfo.getTypeName();
  }

}

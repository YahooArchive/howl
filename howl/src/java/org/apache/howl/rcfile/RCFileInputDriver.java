package org.apache.howl.rcfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.howl.common.HowlUtil;
import org.apache.howl.data.DefaultHowlRecord;
import org.apache.howl.data.HowlRecord;
import org.apache.howl.data.schema.HowlFieldSchema;
import org.apache.howl.data.schema.HowlSchema;
import org.apache.howl.mapreduce.HowlInputStorageDriver;

public class RCFileInputDriver extends HowlInputStorageDriver{


  private SerDe serde;
  private static final Log LOG = LogFactory.getLog(RCFileInputDriver.class);
  private List<HowlFieldSchema> colsInData;
  private StructObjectInspector oi;
  private Map<String,String> partValues;
  private List<HowlFieldSchema> outCols;
  private List<? extends StructField> structFields;
  private Map<String,Integer> namePosMapping;

  @Override
  public InputFormat<? extends WritableComparable, ? extends Writable> getInputFormat(Properties howlProperties) {
    return new RCFileMapReduceInputFormat<LongWritable, BytesRefArrayWritable>();
  }

  @Override
  public void setInputPath(JobContext jobContext, String location) throws IOException {

    super.setInputPath(jobContext, location);
  }

  @Override
  public void setOriginalSchema(JobContext jobContext, HowlSchema dataSchema) throws IOException {

    colsInData = dataSchema.getFields();
    namePosMapping = new HashMap<String, Integer>(colsInData.size());
    int index =0;
    for(HowlFieldSchema field : dataSchema.getFields()){
      namePosMapping.put(field.getName(), index++);
    }
  }

  @Override
  public void setOutputSchema(JobContext jobContext, HowlSchema desiredSchema) throws IOException {

    // Finds out which column ids needs to be projected and set them up for RCFile.
    outCols = desiredSchema.getFields();
    ArrayList<Integer> prjColumns = new ArrayList<Integer>();
    for(HowlFieldSchema prjCol : outCols){
      Integer pos = namePosMapping.get(prjCol.getName().toLowerCase());
      if(pos != null) {
        prjColumns.add(pos);
      }
    }

    Collections.sort(prjColumns);
    ColumnProjectionUtils.setReadColumnIDs(jobContext.getConfiguration(), prjColumns);
  }

  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
  throws IOException {
    partValues = partitionValues;
  }

  @Override
  public HowlRecord convertToHowlRecord(WritableComparable ignored, Writable bytesRefArray) throws IOException {

    // Deserialize bytesRefArray into struct and then convert that struct to
    // HowlRecord.
    ColumnarStruct struct;
    try {
      struct = (ColumnarStruct)serde.deserialize(bytesRefArray);
    } catch (SerDeException e) {
      LOG.error(e.toString(), e);
      throw new IOException(e);
    }

    List<Object> outList = new ArrayList<Object>(outCols.size());

    String colName;
    Integer index;

    for(HowlFieldSchema col : outCols){

      colName = col.getName().toLowerCase();
      index = namePosMapping.get(colName);

      if(index != null){
        StructField field = structFields.get(index);
        outList.add( getTypedObj(oi.getStructFieldData(struct, field), field.getFieldObjectInspector()));
      }

      else {
        outList.add(partValues.get(colName));
      }

    }
    return new DefaultHowlRecord(outList);
  }

  private Object getTypedObj(Object data, ObjectInspector oi) throws IOException{

    // The real work-horse method. We are gobbling up all the laziness benefits
    // of Hive-RCFile by deserializing everything and creating crisp  HowlRecord
    // with crisp Java objects inside it. We have to do it because higher layer
    // may not know how to do it.

    switch(oi.getCategory()){

    case PRIMITIVE:
      return ((PrimitiveObjectInspector)oi).getPrimitiveJavaObject(data);

    case MAP:
      MapObjectInspector moi = (MapObjectInspector)oi;
      Map<?,?> lazyMap = moi.getMap(data);
      ObjectInspector keyOI = moi.getMapKeyObjectInspector();
      ObjectInspector valOI = moi.getMapValueObjectInspector();
      Map<Object,Object> typedMap = new HashMap<Object,Object>(lazyMap.size());
      for(Entry<?,?> e : lazyMap.entrySet()){
        typedMap.put(getTypedObj(e.getKey(), keyOI), getTypedObj(e.getValue(), valOI));
      }
      return typedMap;

    case LIST:
      ListObjectInspector loi = (ListObjectInspector)oi;
      List<?> lazyList = loi.getList(data);
      ObjectInspector elemOI = loi.getListElementObjectInspector();
      List<Object> typedList = new ArrayList<Object>(lazyList.size());
      Iterator<?> itr = lazyList.listIterator();
      while(itr.hasNext()){
        typedList.add(getTypedObj(itr.next(),elemOI));
      }
      return typedList;

    case STRUCT:
      StructObjectInspector soi = (StructObjectInspector)oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      List<Object> typedStruct = new ArrayList<Object>(fields.size());
      for(StructField field : fields){
        typedStruct.add( getTypedObj(soi.getStructFieldData(data, field), field.getFieldObjectInspector()));
      }
      return typedStruct;


    default:
      throw new IOException("Don't know how to deserialize: "+oi.getCategory());

    }
  }

  @Override
  public void initialize(JobContext context,Properties howlProperties)
  throws IOException {

    super.initialize(context, howlProperties);

    // Columnar Serde needs to know names and types of columns it needs to read.
    List<FieldSchema> fields = HowlUtil.getFieldSchemaList(colsInData);
    howlProperties.setProperty(Constants.LIST_COLUMNS,MetaStoreUtils.
        getColumnNamesFromFieldSchema(fields));
    howlProperties.setProperty(Constants.LIST_COLUMN_TYPES, MetaStoreUtils.
        getColumnTypesFromFieldSchema(fields));

    // It seems RCFIle reads and writes nulls differently as compared to default hive.
    // setting these props to match LazySimpleSerde
    howlProperties.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "\\N");
    howlProperties.setProperty(Constants.SERIALIZATION_FORMAT, "1");

    try {
      serde = new ColumnarSerDe();
      serde.initialize(context.getConfiguration(), howlProperties);
      oi = (StructObjectInspector) serde.getObjectInspector();
      structFields = oi.getAllStructFieldRefs();

    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }
}

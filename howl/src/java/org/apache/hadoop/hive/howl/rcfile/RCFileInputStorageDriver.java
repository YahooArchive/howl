package org.apache.hadoop.hive.howl.rcfile;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.data.schema.HowlFieldSchema;
import org.apache.hadoop.hive.howl.data.schema.HowlSchema;
import org.apache.hadoop.hive.howl.mapreduce.HowlInputStorageDriver;
import org.apache.hadoop.hive.howl.mapreduce.HowlUtil;
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
import org.apache.hadoop.util.StringUtils;

public class RCFileInputStorageDriver extends HowlInputStorageDriver{


  private SerDe serde;
  private static final Log LOG = LogFactory.getLog(RCFileInputStorageDriver.class);
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

    // ideally we should just call FileInputFormat.setInputPaths() here - but
    // that won't work since FileInputFormat.setInputPaths() needs
    // a Job object instead of a JobContext which we are handed here

    int length = location.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();

    for (int i=0; i<length; i++) {
      char ch = location.charAt(i);
      switch(ch) {
      case '{' : {
        curlyOpen++;
        if (!globPattern) {
          globPattern = true;
        }
        break;
      }
      case '}' : {
        curlyOpen--;
        if (curlyOpen == 0 && globPattern) {
          globPattern = false;
        }
        break;
      }
      case ',' : {
        if (!globPattern) {
          pathStrings.add(location.substring(pathStart, i));
          pathStart = i + 1 ;
        }
        break;
      }
      }
    }
    pathStrings.add(location.substring(pathStart, length));

    Path[] paths = StringUtils.stringToPath(pathStrings.toArray(new String[0]));

    Configuration conf = jobContext.getConfiguration();

    FileSystem fs = FileSystem.get(conf);
    Path path = paths[0].makeQualified(fs);
    StringBuilder str = new StringBuilder(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < paths.length;i++) {
      str.append(StringUtils.COMMA_STR);
      path = paths[i].makeQualified(fs);
      str.append(StringUtils.escapeString(path.toString()));
    }

    conf.set("mapred.input.dir", str.toString());
  }

  @Override
  public void setOriginalSchema(JobContext jobContext, HowlSchema dataSchema) throws IOException {

    colsInData = dataSchema.getHowlFieldSchemas();
    namePosMapping = new HashMap<String, Integer>(colsInData.size());
    int index =0;
    for(FieldSchema field : dataSchema.getHowlFieldSchemas()){
      namePosMapping.put(field.getName(), index++);
    }
  }

  @Override
  public void setOutputSchema(JobContext jobContext, HowlSchema desiredSchema) throws IOException {

    // Finds out which column ids needs to be projected and set them up for RCFile.
    outCols = desiredSchema.getHowlFieldSchemas();

    ArrayList<Integer> prjColumns = new ArrayList<Integer>();
    for(FieldSchema prjCol : outCols){
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

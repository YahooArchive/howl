package org.apache.hadoop.hive.howl.drivers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.howl.data.DefaultHowlRecord;
import org.apache.hadoop.hive.howl.data.HowlRecord;
import org.apache.hadoop.hive.howl.mapreduce.HowlInputStorageDriver;
import org.apache.hadoop.hive.howl.mapreduce.LoaderInfo;
import org.apache.hadoop.hive.howl.mapreduce.HowlInputFormat.HowlOperation;
import org.apache.hadoop.hive.io.RCFileMapReduceInputFormat;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
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
  private List<FieldSchema> fSchemasOfAll;
  private StructObjectInspector oi;

  @Override
  public InputFormat<? extends WritableComparable, ? extends Writable> getInputFormat(LoaderInfo loaderInfo) {
    return new RCFileMapReduceInputFormat<LongWritable, BytesRefArrayWritable>();
  }

  @Override
  public void setInputPath(JobContext jobContext, String location) throws IOException {

    // ideally we should just call ps.setLocation() here - but that won't
    // work since ps.setLocation() calls FileInputFormat.setInputPaths()
    // which needs a Job object instead of a JobContext which we are handed
    // here

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
  public void setOriginalSchema(JobContext jobContext, Schema hiveSchema) throws IOException {

    fSchemasOfAll = hiveSchema.getFieldSchemas();
  }

  @Override
  public boolean setOutputSchema(JobContext jobContext, Schema hiveSchema) throws IOException {

    List<FieldSchema> fSchemasOfPrj = hiveSchema.getFieldSchemas();
    Set<String> prjColNames = new HashSet<String>(fSchemasOfPrj.size());

    for(FieldSchema fs : fSchemasOfPrj){
      prjColNames.add(fs.getName());
    }

    ArrayList<Integer> prjColumnPos = new ArrayList<Integer>();
    for(int index = 0; index < fSchemasOfAll.size(); index++){
      if(prjColNames.contains(fSchemasOfAll.get(index).getName())) {
        prjColumnPos.add(index);
      }
    }

    Collections.sort(prjColumnPos);
    ColumnProjectionUtils.setReadColumnIDs(jobContext.getConfiguration(), prjColumnPos);
    return true;
  }

  @Override
  public void setPartitionValues(JobContext jobContext, Map<String, String> partitionValues)
  throws IOException {

  }

  @Override
  public boolean isFeatureSupported(HowlOperation operation) throws IOException {

    return operation.equals(HowlOperation.PROJECTION_PUSHDOWN) ? true : false;
  }

  @Override
  public HowlRecord convertValueToHowlRecord(Writable bytesRefArray) throws IOException {

     ColumnarStruct struct;
    try {
      struct = (ColumnarStruct)serde.deserialize(bytesRefArray);
    } catch (SerDeException e) {
      LOG.error(e.toString(), e);
      throw new IOException(e);
    }

    List<? extends StructField> fields = oi.getAllStructFieldRefs();
    List<Object> outList = new ArrayList<Object>(fields.size());

    for(StructField field : fields){
      outList.add(getTypedObj(oi.getStructFieldData(struct, field), field.getFieldObjectInspector()));
    }

    return new DefaultHowlRecord(outList);
  }


  private Object getTypedObj(Object data, ObjectInspector oi) throws IOException{


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
  public void initialize(JobContext context, LoaderInfo loaderInfo)
  throws IOException {

    super.initialize(context, loaderInfo);

    Properties props = new Properties();
    props.setProperty(Constants.LIST_COLUMNS,MetaStoreUtils.getColumnNamesFromFieldSchema(fSchemasOfAll));
    props.setProperty(Constants.LIST_COLUMN_TYPES, MetaStoreUtils.getColumnTypesFromFieldSchema(fSchemasOfAll));

    try {
      serde = new ColumnarSerDe();
      serde.initialize(context.getConfiguration(), props);
      oi = (StructObjectInspector) serde.getObjectInspector();

    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }
}

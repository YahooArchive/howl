package org.apache.howl.common;

public final class HowlConstants {

  /** The key for the input storage driver class name */
  public static final String HOWL_ISD_CLASS = "howl.isd";

  /** The key for the output storage driver class name */
  public static final String HOWL_OSD_CLASS = "howl.osd";

  public static final String HIVE_RCFILE_IF_CLASS = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
  public static final String HIVE_RCFILE_OF_CLASS = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat";
  public static final String HOWL_RCFILE_ISD_CLASS = "org.apache.howl.rcfile.RCFileInputDriver";
  public static final String HOWL_RCFILE_OSD_CLASS = "org.apache.howl.rcfile.RCFileOutputDriver";

  private HowlConstants() { // restrict instantiation
  }

  public static final String HOWL_TABLE_SCHEMA = "howl.table.schema";

  public static final String HOWL_METASTORE_URI = "howl.metastore.uri";

  public static final String HOWL_PERMS = "howl.perms";

  public static final String HOWL_GROUP = "howl.group";

  public static final String HOWL_CREATE_TBL_NAME = "howl.create.tbl.name";

  public static final String HOWL_CREATE_DB_NAME = "howl.create.db.name";

  public static final String HOWL_METASTORE_PRINCIPAL = "howl.metastore.principal";
}

package org.apache.howl.common;

public final class HowlConstants {

  /** The key for the input storage driver class name */
  public static final String HOWL_ISD_CLASS = "howl.isd";

  /** The key for the output storage driver class name */
  public static final String HOWL_OSD_CLASS = "howl.osd";

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

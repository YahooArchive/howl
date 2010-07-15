/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.conf;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Hive Configuration.
 */
public class HiveConf extends Configuration {

  protected String hiveJar;
  protected Properties origProp;
  protected String auxJars;
  private static final Log l4j = LogFactory.getLog(HiveConf.class);

  /**
   * Metastore related options that the db is initialized against. When a conf
   * var in this is list is changed, the metastore instance for the CLI will
   * be recreated so that the change will take effect.
   */
  public static final HiveConf.ConfVars[] metaVars = {
      HiveConf.ConfVars.METASTOREDIRECTORY,
      HiveConf.ConfVars.METASTOREWAREHOUSE,
      HiveConf.ConfVars.METASTOREURIS,
      HiveConf.ConfVars.METATORETHRIFTRETRIES,
      HiveConf.ConfVars.METASTOREPWD,
      HiveConf.ConfVars.METASTORECONNECTURLHOOK,
      HiveConf.ConfVars.METASTORECONNECTURLKEY,
      HiveConf.ConfVars.METASTOREATTEMPTS,
      HiveConf.ConfVars.METASTOREINTERVAL,
      HiveConf.ConfVars.METASTOREFORCERELOADCONF,
      };

  /**
   * ConfVars.
   *
   */
  public static enum ConfVars {
    // QL execution stuff
    SCRIPTWRAPPER("hive.exec.script.wrapper", null),
    PLAN("hive.exec.plan", null),
    SCRATCHDIR("hive.exec.scratchdir", "/tmp/" + System.getProperty("user.name") + "/hive"),
    SUBMITVIACHILD("hive.exec.submitviachild", false),
    SCRIPTERRORLIMIT("hive.exec.script.maxerrsize", 100000),
    ALLOWPARTIALCONSUMP("hive.exec.script.allow.partial.consumption", false),
    COMPRESSRESULT("hive.exec.compress.output", false),
    COMPRESSINTERMEDIATE("hive.exec.compress.intermediate", false),
    COMPRESSINTERMEDIATECODEC("hive.intermediate.compression.codec", ""),
    COMPRESSINTERMEDIATETYPE("hive.intermediate.compression.type", ""),
    BYTESPERREDUCER("hive.exec.reducers.bytes.per.reducer", (long) (1000 * 1000 * 1000)),
    MAXREDUCERS("hive.exec.reducers.max", 999),
    PREEXECHOOKS("hive.exec.pre.hooks", ""),
    POSTEXECHOOKS("hive.exec.post.hooks", ""),
    EXECPARALLEL("hive.exec.parallel", false), // parallel query launching
    EXECPARALLETHREADNUMBER("hive.exec.parallel.thread.number", 8),
    HIVESPECULATIVEEXECREDUCERS("hive.mapred.reduce.tasks.speculative.execution", true),
    HIVECOUNTERSPULLINTERVAL("hive.exec.counters.pull.interval", 1000L),
    DYNAMICPARTITIONING("hive.exec.dynamic.partition", false),
    DYNAMICPARTITIONINGMODE("hive.exec.dynamic.partition.mode", "strict"),
    DYNAMICPARTITIONMAXPARTS("hive.exec.max.dynamic.partitions", 1000),
    DYNAMICPARTITIONMAXPARTSPERNODE("hive.exec.max.dynamic.partitions.pernode", 100),
    DEFAULTPARTITIONNAME("hive.exec.default.partition.name", "__HIVE_DEFAULT_PARTITION__"),

    // hadoop stuff
    HADOOPBIN("hadoop.bin.path", System.getenv("HADOOP_HOME") + "/bin/hadoop"),
    HADOOPCONF("hadoop.config.dir", System.getenv("HADOOP_HOME") + "/conf"),
    HADOOPFS("fs.default.name", "file:///"),
    HADOOPMAPFILENAME("map.input.file", null),
    HADOOPMAPREDINPUTDIR("mapred.input.dir", null),
    HADOOPMAPREDINPUTDIRRECURSIVE("mapred.input.dir.recursive", false),
    HADOOPJT("mapred.job.tracker", "local"),
    HADOOPNUMREDUCERS("mapred.reduce.tasks", 1),
    HADOOPJOBNAME("mapred.job.name", null),
    HADOOPSPECULATIVEEXECREDUCERS("mapred.reduce.tasks.speculative.execution", false),

    // Metastore stuff. Be sure to update HiveConf.metaVars when you add
    // something here!
    METASTOREDIRECTORY("hive.metastore.metadb.dir", ""),
    METASTOREWAREHOUSE("hive.metastore.warehouse.dir", ""),
    METASTOREURIS("hive.metastore.uris", ""),
    // Number of times to retry a connection to a Thrift metastore server
    METATORETHRIFTRETRIES("hive.metastore.connect.retries", 3),
    METASTOREPWD("javax.jdo.option.ConnectionPassword", ""),
    // Class name of JDO connection url hook
    METASTORECONNECTURLHOOK("hive.metastore.ds.connection.url.hook", ""),
    // Name of the connection url in the configuration
    METASTORECONNECTURLKEY("javax.jdo.option.ConnectionURL", ""),
    // Number of attempts to retry connecting after there is a JDO datastore err
    METASTOREATTEMPTS("hive.metastore.ds.retry.attempts", 1),
    // Number of miliseconds to wait between attepting
    METASTOREINTERVAL("hive.metastore.ds.retry.interval", 1000),
    // Whether to force reloading of the metastore configuration (including
    // the connection URL, before the next metastore query that accesses the
    // datastore. Once reloaded, the  this value is reset to false. Used for
    // testing only.
    METASTOREFORCERELOADCONF("hive.metastore.force.reload.conf", false),
    METASTORESERVERMINTHREADS("hive.metastore.server.min.threads", 200),
    METASTORESERVERMAXTHREADS("hive.metastore.server.max.threads", Integer.MAX_VALUE),
    METASTORE_TCP_KEEP_ALIVE("hive.metastore.server.tcp.keepalive", true),
    // Intermediate dir suffixes used for archiving. Not important what they
    // are, as long as collisions are avoided
    METASTORE_INT_ORIGINAL("hive.metastore.archive.intermediate.original",
        "_INTERMEDIATE_ORIGINAL"),
    METASTORE_INT_ARCHIVED("hive.metastore.archive.intermediate.archived",
        "_INTERMEDIATE_ARCHIVED"),
    METASTORE_INT_EXTRACTED("hive.metastore.archive.intermediate.extracted",
        "_INTERMEDIATE_EXTRACTED"),

    // CLI
    CLIIGNOREERRORS("hive.cli.errors.ignore", false),

    // Things we log in the jobconf

    // session identifier
    HIVESESSIONID("hive.session.id", ""),
    // whether session is running in silent mode or not
    HIVESESSIONSILENT("hive.session.silent", false),

    // query being executed (multiple per session)
    HIVEQUERYSTRING("hive.query.string", ""),

    // id of query being executed (multiple per session)
    HIVEQUERYID("hive.query.id", ""),

    // id of the mapred plan being executed (multiple per query)
    HIVEPLANID("hive.query.planid", ""),
        // max jobname length
    HIVEJOBNAMELENGTH("hive.jobname.length", 50),

    // hive jar
    HIVEJAR("hive.jar.path", ""),
    HIVEAUXJARS("hive.aux.jars.path", ""),

    // hive added files and jars
    HIVEADDEDFILES("hive.added.files.path", ""),
    HIVEADDEDJARS("hive.added.jars.path", ""),
    HIVEADDEDARCHIVES("hive.added.archives.path", ""),

    // for hive script operator
    HIVETABLENAME("hive.table.name", ""),
    HIVEPARTITIONNAME("hive.partition.name", ""),
    HIVESCRIPTAUTOPROGRESS("hive.script.auto.progress", false),
    HIVESCRIPTIDENVVAR("hive.script.operator.id.env.var", "HIVE_SCRIPT_OPERATOR_ID"),
    HIVEMAPREDMODE("hive.mapred.mode", "nonstrict"),
    HIVEALIAS("hive.alias", ""),
    HIVEMAPSIDEAGGREGATE("hive.map.aggr", "true"),
    HIVEGROUPBYSKEW("hive.groupby.skewindata", "false"),
    HIVEJOINEMITINTERVAL("hive.join.emit.interval", 1000),
    HIVEJOINCACHESIZE("hive.join.cache.size", 25000),
    HIVEMAPJOINBUCKETCACHESIZE("hive.mapjoin.bucket.cache.size", 100),
    HIVEMAPJOINROWSIZE("hive.mapjoin.size.key", 10000),
    HIVEMAPJOINCACHEROWS("hive.mapjoin.cache.numrows", 25000),
    HIVEGROUPBYMAPINTERVAL("hive.groupby.mapaggr.checkinterval", 100000),
    HIVEMAPAGGRHASHMEMORY("hive.map.aggr.hash.percentmemory", (float) 0.5),
    HIVEMAPAGGRHASHMINREDUCTION("hive.map.aggr.hash.min.reduction", (float) 0.5),

    // for hive udtf operator
    HIVEUDTFAUTOPROGRESS("hive.udtf.auto.progress", false),

    // Default file format for CREATE TABLE statement
    // Options: TextFile, SequenceFile
    HIVEDEFAULTFILEFORMAT("hive.default.fileformat", "TextFile"),
    HIVECHECKFILEFORMAT("hive.fileformat.check", true),

    //Location of Hive run time structured log file
    HIVEHISTORYFILELOC("hive.querylog.location", "/tmp/" + System.getProperty("user.name")),

    // Default serde and record reader for user scripts
    HIVESCRIPTSERDE("hive.script.serde", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    HIVESCRIPTRECORDREADER("hive.script.recordreader",
        "org.apache.hadoop.hive.ql.exec.TextRecordReader"),
    HIVESCRIPTRECORDWRITER("hive.script.recordwriter",
        "org.apache.hadoop.hive.ql.exec.TextRecordWriter"),

    // HWI
    HIVEHWILISTENHOST("hive.hwi.listen.host", "0.0.0.0"),
    HIVEHWILISTENPORT("hive.hwi.listen.port", "9999"),
    HIVEHWIWARFILE("hive.hwi.war.file", System.getenv("HWI_WAR_FILE")),

    // mapper/reducer memory in local mode
    HIVEHADOOPMAXMEM("hive.mapred.local.mem", 0),

    // test mode in hive mode
    HIVETESTMODE("hive.test.mode", false),
    HIVETESTMODEPREFIX("hive.test.mode.prefix", "test_"),
    HIVETESTMODESAMPLEFREQ("hive.test.mode.samplefreq", 32),
    HIVETESTMODENOSAMPLE("hive.test.mode.nosamplelist", ""),

    HIVEMERGEMAPFILES("hive.merge.mapfiles", true),
    HIVEMERGEMAPREDFILES("hive.merge.mapredfiles", false),
    HIVEMERGEMAPFILESSIZE("hive.merge.size.per.task", (long) (256 * 1000 * 1000)),
    HIVEMERGEMAPFILESAVGSIZE("hive.merge.smallfiles.avgsize", (long) (16 * 1000 * 1000)),

    HIVESKEWJOIN("hive.optimize.skewjoin", false),
    HIVESKEWJOINKEY("hive.skewjoin.key", 1000000),
    HIVESKEWJOINMAPJOINNUMMAPTASK("hive.skewjoin.mapjoin.map.tasks", 10000),
    HIVESKEWJOINMAPJOINMINSPLIT("hive.skewjoin.mapjoin.min.split", 33554432), //32M
    MAPREDMINSPLITSIZE("mapred.min.split.size", 1),

    HIVESENDHEARTBEAT("hive.heartbeat.interval", 1000),
    HIVEMAXMAPJOINSIZE("hive.mapjoin.maxsize", 100000),

    HIVEJOBPROGRESS("hive.task.progress", false),

    HIVEINPUTFORMAT("hive.input.format", ""),

    HIVEENFORCEBUCKETING("hive.enforce.bucketing", false),
    HIVEENFORCESORTING("hive.enforce.sorting", false),
    HIVEPARTITIONER("hive.mapred.partitioner", "org.apache.hadoop.hive.ql.io.DefaultHivePartitioner"),

    HIVESCRIPTOPERATORTRUST("hive.exec.script.trust", false),

    HIVE_COMBINE_INPUT_FORMAT_SUPPORTS_SPLITTABLE("hive.hadoop.supports.splittable.combineinputformat", false),

    // Optimizer
    HIVEOPTCP("hive.optimize.cp", true), // column pruner
    HIVEOPTPPD("hive.optimize.ppd", true), // predicate pushdown
    HIVEOPTGROUPBY("hive.optimize.groupby", true), // optimize group by
    HIVEOPTBUCKETMAPJOIN("hive.optimize.bucketmapjoin", false), // optimize bucket map join
    HIVEOPTSORTMERGEBUCKETMAPJOIN("hive.optimize.bucketmapjoin.sortedmerge", false), // try to use sorted merge bucket map join
    HIVEOPTREDUCEDEDUPLICATION("hive.optimize.reducededuplication", true),

    // For HBase storage handler
    HIVE_HBASE_WAL_ENABLED("hive.hbase.wal.enabled", true),

    // For har files
    HIVEARCHIVEENABLED("hive.archive.enabled", false),
    HIVEHARPARENTDIRSETTABLE("hive.archive.har.parentdir.settable", false),
    ;

    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final Class<?> valClass;
    public final boolean defaultBoolVal;

    ConfVars(String varname, String defaultVal) {
      this.varname = varname;
      this.valClass = String.class;
      this.defaultVal = defaultVal;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.valClass = Integer.class;
      this.defaultVal = null;
      this.defaultIntVal = defaultIntVal;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, long defaultLongVal) {
      this.varname = varname;
      this.valClass = Long.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = defaultLongVal;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, float defaultFloatVal) {
      this.varname = varname;
      this.valClass = Float.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = defaultFloatVal;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.valClass = Boolean.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = defaultBoolVal;
    }

    @Override
    public String toString() {
      return varname;
    }
  }

  public static int getIntVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Integer.class);
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public static void setIntVar(Configuration conf, ConfVars var, int val) {
    assert (var.valClass == Integer.class);
    conf.setInt(var.varname, val);
  }

  public int getIntVar(ConfVars var) {
    return getIntVar(this, var);
  }

  public void setIntVar(ConfVars var, int val) {
    setIntVar(this, var, val);
  }

  public static long getLongVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Long.class);
    return conf.getLong(var.varname, var.defaultLongVal);
  }

  public static void setLongVar(Configuration conf, ConfVars var, long val) {
    assert (var.valClass == Long.class);
    conf.setLong(var.varname, val);
  }

  public long getLongVar(ConfVars var) {
    return getLongVar(this, var);
  }

  public void setLongVar(ConfVars var, long val) {
    setLongVar(this, var, val);
  }

  public static float getFloatVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Float.class);
    return conf.getFloat(var.varname, var.defaultFloatVal);
  }

  public static void setFloatVar(Configuration conf, ConfVars var, float val) {
    assert (var.valClass == Float.class);
    ShimLoader.getHadoopShims().setFloatConf(conf, var.varname, val);
  }

  public float getFloatVar(ConfVars var) {
    return getFloatVar(this, var);
  }

  public void setFloatVar(ConfVars var, float val) {
    setFloatVar(this, var, val);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Boolean.class);
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public static void setBoolVar(Configuration conf, ConfVars var, boolean val) {
    assert (var.valClass == Boolean.class);
    conf.setBoolean(var.varname, val);
  }

  public boolean getBoolVar(ConfVars var) {
    return getBoolVar(this, var);
  }

  public void setBoolVar(ConfVars var, boolean val) {
    setBoolVar(this, var, val);
  }

  public static String getVar(Configuration conf, ConfVars var) {
    assert (var.valClass == String.class);
    return conf.get(var.varname, var.defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    assert (var.valClass == String.class);
    conf.set(var.varname, val);
  }

  public String getVar(ConfVars var) {
    return getVar(this, var);
  }

  public void setVar(ConfVars var, String val) {
    setVar(this, var, val);
  }

  public void logVars(PrintStream ps) {
    for (ConfVars one : ConfVars.values()) {
      ps.println(one.varname + "=" + ((get(one.varname) != null) ? get(one.varname) : ""));
    }
  }

  public HiveConf() {
    super();
  }

  public HiveConf(Class<?> cls) {
    super();
    initialize(cls);
  }

  public HiveConf(Configuration other, Class<?> cls) {
    super(other);
    initialize(cls);
  }

  private Properties getUnderlyingProps() {
    Iterator<Map.Entry<String, String>> iter = this.iterator();
    Properties p = new Properties();
    while (iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      p.setProperty(e.getKey(), e.getValue());
    }
    return p;
  }

  private void initialize(Class<?> cls) {
    hiveJar = (new JobConf(cls)).getJar();

    // preserve the original configuration
    origProp = getUnderlyingProps();

    // let's add the hive configuration
    URL hconfurl = getClassLoader().getResource("hive-default.xml");
    if (hconfurl == null) {
      l4j.debug("hive-default.xml not found.");
    } else {
      addResource(hconfurl);
    }
    URL hsiteurl = getClassLoader().getResource("hive-site.xml");
    if (hsiteurl == null) {
      l4j.debug("hive-site.xml not found.");
    } else {
      addResource(hsiteurl);
    }

    // if hadoop configuration files are already in our path - then define
    // the containing directory as the configuration directory
    URL hadoopconfurl = getClassLoader().getResource("hadoop-default.xml");
    if (hadoopconfurl == null) {
      hadoopconfurl = getClassLoader().getResource("hadoop-site.xml");
    }
    if (hadoopconfurl != null) {
      String conffile = hadoopconfurl.getPath();
      this.setVar(ConfVars.HADOOPCONF, conffile.substring(0, conffile.lastIndexOf('/')));
    }

    applySystemProperties();

    // if the running class was loaded directly (through eclipse) rather than through a
    // jar then this would be needed
    if (hiveJar == null) {
      hiveJar = this.get(ConfVars.HIVEJAR.varname);
    }

    if (auxJars == null) {
      auxJars = this.get(ConfVars.HIVEAUXJARS.varname);
    }

  }

  public void applySystemProperties() {
    for (ConfVars oneVar : ConfVars.values()) {
      if (System.getProperty(oneVar.varname) != null) {
        if (System.getProperty(oneVar.varname).length() > 0) {
          this.set(oneVar.varname, System.getProperty(oneVar.varname));
        }
      }
    }
  }

  public Properties getChangedProperties() {
    Properties ret = new Properties();
    Properties newProp = getUnderlyingProps();

    for (Object one : newProp.keySet()) {
      String oneProp = (String) one;
      String oldValue = origProp.getProperty(oneProp);
      if (!StringUtils.equals(oldValue, newProp.getProperty(oneProp))) {
        ret.setProperty(oneProp, newProp.getProperty(oneProp));
      }
    }
    return (ret);
  }

  public Properties getAllProperties() {
    return getUnderlyingProps();
  }

  public String getJar() {
    return hiveJar;
  }

  /**
   * @return the auxJars
   */
  public String getAuxJars() {
    return auxJars;
  }

  /**
   * @param auxJars the auxJars to set
   */
  public void setAuxJars(String auxJars) {
    this.auxJars = auxJars;
    setVar(this, ConfVars.HIVEAUXJARS, auxJars);
  }

  /**
   * @return the user name set in hadoop.job.ugi param or the current user from System
   * @throws IOException
   */
  public String getUser() throws IOException {
    try {
      UserGroupInformation ugi = UserGroupInformation.readFrom(this);
      if (ugi == null) {
        ugi = UserGroupInformation.login(this);
      }
      return ugi.getUserName();
    } catch (LoginException e) {
      throw (IOException) new IOException().initCause(e);
    }
  }

  public static String getColumnInternalName(int pos) {
    return "_col" + pos;
  }
}

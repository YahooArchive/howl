/**
 *
 */
package org.apache.hadoop.hive.howl.cli;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.commons.cli2.Argument;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.option.PropertyOption;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.howl.cli.SemanticAnalysis.HowlSemanticAnalyzer;
import org.apache.hadoop.hive.howl.common.HowlConstants;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.SetProcessor;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 *
 */
public class HowlCli {

  public static void main(String[] args) {

    SessionState.initHiveLog4j();

    CliSessionState ss = new CliSessionState(new HiveConf(SessionState.class));
    ss.in = System.in;
    try {
      ss.out = new PrintStream(System.out, true, "UTF-8");
      ss.err = new PrintStream(System.err, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      System.exit(1);
    }

    HiveConf conf = ss.getConf();

    HiveConf.setVar(conf, ConfVars.SEMANTIC_ANALYZER_HOOK, HowlSemanticAnalyzer.class.getName());

    SessionState.start(ss);

    DefaultOptionBuilder builder = new DefaultOptionBuilder("-", "--", false);
    ArgumentBuilder argBuilder = new ArgumentBuilder();
    // -e
    Option execOption = createOptionWithArg(builder, "exec", "e", "execute the following command",argBuilder.withMinimum(1).withMaximum(1).create());
    // -f
    Option fileOption = createOptionWithArg(builder, "file", "f","execute commands from the following file", argBuilder.withMinimum(1).withMaximum(1).create());
    // -g
    Option grpOption = createOptionWithArg(builder, "group", "g","group for the db/table specified in CREATE statement", argBuilder.withMinimum(1).withMaximum(1).create());
    // -p
    Option permOption = createOptionWithArg(builder, "perms", "p","permissions for the db/table specified in CREATE statement",
        argBuilder.withMinimum(1).withMaximum(1).create());

    builder.reset();
    Option isHelpOption =  builder.withShortName("h").withLongName("help").withDescription("help").create();
    new PropertyOption();
    Group allOptions = new GroupBuilder().withOption(isHelpOption).withOption(execOption).withOption(fileOption).withOption(grpOption).withOption(permOption).create();

    Parser parser = new Parser();
    parser.setGroup(allOptions);
    CommandLine cmdLine = null;

    try {
      cmdLine  = parser.parse(args);

    } catch (OptionException e1) {
      printErrString(null, System.err);
      System.exit(1);
    }
    // -e
    String execString = (String) cmdLine.getValue(execOption);
    // -f
    String fileName = (String) cmdLine.getValue(fileOption);
    // -h
    if (cmdLine.hasOption(isHelpOption)) {
      printErrString(null, System.out);
      System.exit(1);
    }

    if (execString != null && fileName != null) {
      printErrString("Please specify either -e or -f option.", System.err);
      System.exit(1);
    }

    // -p
    String perms = (String) cmdLine.getValue(permOption);
    if(perms != null){
      perms = perms.trim();
      if(perms.matches("^\\s*([r,w,x,-]{9})\\s*$")){
        conf.set(HowlConstants.HOWL_PERMS,"d"+perms);
      }
      else if(perms.matches("^\\s*([0-7]{3})\\s*$")){
          conf.set(HowlConstants.HOWL_PERMS, "d"+new FsPermission(Short.decode("0"+perms)).toString());
        }
      else{
        ss.err.println("Invalid permission specification: "+perms);
        System.exit(1);
      }
    }

    // -g
    String grp = (String) cmdLine.getValue(grpOption);
    if(grp != null){
      conf.set(HowlConstants.HOWL_GROUP, grp);
    }

    if (execString != null) {
      System.exit(processLine(execString));
    }

    try {
      if (fileName != null) {
        System.exit(processFile(fileName));
      }
    } catch (FileNotFoundException e) {
      ss.err.println("Input file not found. (" + e.getMessage() + ")");
      System.exit(1);
    } catch (IOException e) {
      ss.err.println("Could not open input file for reading. (" + e.getMessage() + ")");
      System.exit(1);
    }

    // -h
    printErrString(null, System.err);
  }

  private static int processLine(String line) {
    int ret = 0;

    String command = "";
    for (String oneCmd : line.split(";")) {

      if (StringUtils.endsWith(oneCmd, "\\")) {
        command += StringUtils.chop(oneCmd) + ";";
        continue;
      } else {
        command += oneCmd;
      }
      if (StringUtils.isBlank(command)) {
        continue;
      }

      ret = processCmd(command);
      command = "";
    }
    return ret;
  }

  private static int processFile(String fileName) throws IOException {
    FileReader fileReader = null;
    BufferedReader reader = null;
    try {
      fileReader = new FileReader(fileName);
      reader = new BufferedReader(fileReader);
      String line;
      StringBuilder qsb = new StringBuilder();

      while ((line = reader.readLine()) != null) {
        qsb.append(line + "\n");
      }

      return (processLine(qsb.toString()));
    } finally {
      if (fileReader != null) {
        fileReader.close();
      }
      if(reader != null) {
        reader.close();
      }
    }
  }

  private static int processCmd(String cmd){

    SessionState ss = SessionState.get();
    long start = System.currentTimeMillis();

    cmd = cmd.trim();
    String firstToken = cmd.split("\\s+")[0].trim();

    if(firstToken.equalsIgnoreCase("set")){
      return new SetProcessor().run(cmd.substring(firstToken.length()).trim()).getResponseCode();
    }

    Driver driver = new Driver();

    int ret = driver.run(cmd).getResponseCode();
    if (ret == 0){
      ret = setFSPermsNGrp(ss);
      ss.getConf().set(HowlConstants.HOWL_CREATE_DB_NAME, null);
      ss.getConf().set(HowlConstants.HOWL_CREATE_TBL_NAME, null);
    }
    if (ret != 0) {
      driver.close();
      System.exit(ret);
    }

    ArrayList<String> res = new ArrayList<String>();
    try {
      while (driver.getResults(res)) {
        for (String r : res) {
          ss.out.println(r);
        }
        res.clear();
      }
    } catch (IOException e) {
      ss.err.println("Failed with exception " + e.getClass().getName() + ":"
          + e.getMessage() + "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      ret = 1;
    }

    int cret = driver.close();
    if (ret == 0) {
      ret = cret;
    }

    long end = System.currentTimeMillis();
    if (end > start) {
      double timeTaken = (end - start) / 1000.0;
      ss.err.println("Time taken: " + timeTaken + " seconds");
    }
    return ret;
  }

  private static int setFSPermsNGrp(SessionState ss) {

    Configuration conf =ss.getConf();

    String tblName = conf.get(HowlConstants.HOWL_CREATE_TBL_NAME,null);
    String dbName = conf.get(HowlConstants.HOWL_CREATE_DB_NAME, null);
    String grp = conf.get(HowlConstants.HOWL_GROUP,null);
    String permsStr = conf.get(HowlConstants.HOWL_PERMS,null);

    if(null == tblName && null == dbName){
      // it wasn't create db/table
      return 0;
    }

    if(null == grp && null == permsStr) {
      // there were no grp and perms to begin with.
      return 0;
    }

    FsPermission perms = FsPermission.valueOf(permsStr);
    if(tblName != null){
      Hive db = null;
      try{
        db = Hive.get();
        Table tbl =  db.getTable(tblName);
        Path tblPath = tbl.getPath();

        FileSystem fs = tblPath.getFileSystem(conf);
        if(null != perms){
          fs.setPermission(tblPath, perms);
        }
        if(null != grp){
          fs.setOwner(tblPath, null, grp);
        }
        return 0;

      } catch (Exception e){
          ss.err.println(String.format("Failed to set permissions/groups on TABLE: <%s> /n%s",tblName,e.getMessage()));
          try {  // We need to drop the table.
            if(null != db){ db.dropTable(tblName); }
          } catch (HiveException he) {
            ss.err.println(String.format("Failed to drop TABLE <%s> after failing to set permissions/groups on it. /n%s",tblName,e.getMessage()));
          }
          return 1;
      }
    }
    else{
      // looks like a db operation
      if (dbName == null || dbName.equals(MetaStoreUtils.DEFAULT_DATABASE_NAME)){
        // We dont set perms or groups for default dir.
        return 0;
      }
      else{
        try{
          Path dbPath = new Warehouse(conf).getDefaultDatabasePath(dbName);
          FileSystem fs = dbPath.getFileSystem(conf);
          if(perms != null){
            fs.setPermission(dbPath, perms);
          }
          if(null != grp){
            fs.setOwner(dbPath, null, grp);
          }
          return 0;
        } catch (Exception e){
          ss.err.println(String.format("Failed to set permissions and/or group on DB: <%s> /n%s", dbName, e.getMessage()));
          try {
            Hive.get().dropDatabase(dbName);
          } catch (Exception e1) {
            ss.err.println(String.format("Failed to drop DB <%s> after failing to set permissions/group on it. /n%s", dbName, e1.getMessage()));
          }
          return 1;
        }
      }
    }
  }

  /**
   * @param ps TODO
   *
   */
  private static void printErrString(String str, PrintStream ps) {
    ps.println(str == null ? "Usage: howl { -e \"<query>\" | -f \"<filepath>\" } [ -g \"<group>\" ] [ -p \"<perms>\" ] " : str);
  }

  private static Option createOptionWithArg(DefaultOptionBuilder builder, String longName,
      String shortName, String desc, Argument arg) {

    builder.reset();
    DefaultOptionBuilder dob = builder.withShortName(shortName).withArgument(arg).withDescription(desc);

    if (longName != null) {
      dob = dob.withLongName(longName);
    }
    return dob.create();
  }
}

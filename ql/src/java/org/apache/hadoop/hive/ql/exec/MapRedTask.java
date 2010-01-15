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

package org.apache.hadoop.hive.ql.exec;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Utilities.StreamPrinter;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * Alternate implementation (to ExecDriver) of spawning a mapreduce task that runs it from
 * a separate jvm. The primary issue with this is the inability to control logging from
 * a separate jvm in a consistent manner
 **/
public class MapRedTask extends Task<mapredWork> implements Serializable {
    
  private static final long serialVersionUID = 1L;

  final static String hadoopMemKey = "HADOOP_HEAPSIZE";
  final static String hadoopOptsKey = "HADOOP_OPTS";
  final static String HIVE_SYS_PROP[] = {"build.dir", "build.dir.hive"}; 
  
  public MapRedTask() {
    super();
  }
  
  public int execute() {

    try {
      // enable assertion
      String hadoopExec = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
      String hiveJar = conf.getJar();

      String libJarsOption;
      {
        String addedJars = ExecDriver.getResourceFiles(conf, SessionState.ResourceType.JAR);
        conf.setVar(ConfVars.HIVEADDEDJARS, addedJars);

        String auxJars = conf.getAuxJars();
        // Put auxjars and addedjars together into libjars
        if (StringUtils.isEmpty(addedJars)) {
          if (StringUtils.isEmpty(auxJars)) {
            libJarsOption = " ";
          } else {
            libJarsOption = " -libjars " + auxJars + " ";
          }
        } else {
          if (StringUtils.isEmpty(auxJars)) {
            libJarsOption = " -libjars " + addedJars + " ";
          } else {
            libJarsOption = " -libjars " + addedJars + "," + auxJars + " ";
          }   
        }
      }

      // Generate the hiveConfArgs after potentially adding the jars
      String hiveConfArgs = ExecDriver.generateCmdLine(conf);
      File scratchDir = new File(conf.getVar(HiveConf.ConfVars.SCRATCHDIR));
      
      mapredWork plan = getWork();

      File planFile = File.createTempFile("plan", ".xml", scratchDir);
      LOG.info("Generating plan file " + planFile.toString());
      FileOutputStream out = new FileOutputStream(planFile);
      Utilities.serializeMapRedWork(plan, out);

      String isSilent = "true".equalsIgnoreCase(System.getProperty("test.silent"))
                        ? "-silent" : "";

      String jarCmd;
      if(ShimLoader.getHadoopShims().usesJobShell()) {
        jarCmd = libJarsOption + hiveJar + " " + ExecDriver.class.getName();
      } else {
        jarCmd = hiveJar + " " + ExecDriver.class.getName() + libJarsOption;
      }

      String cmdLine = hadoopExec + " jar " + jarCmd + 
        " -plan " + planFile.toString() + " " + isSilent + " " + hiveConfArgs; 
      
      String files = ExecDriver.getResourceFiles(conf, SessionState.ResourceType.FILE);
      if(!files.isEmpty()) {
        cmdLine = cmdLine + " -files " + files;
      }

      LOG.info("Executing: " + cmdLine);
      Process executor = null;

      // Inherit Java system variables
      String hadoopOpts;
      {
        StringBuilder sb = new StringBuilder();
        Properties p = System.getProperties();
        for (int k = 0; k < HIVE_SYS_PROP.length; k++) {
          if (p.containsKey(HIVE_SYS_PROP[k])) {
            sb.append(" -D" + HIVE_SYS_PROP[k] + "=" + p.getProperty(HIVE_SYS_PROP[k]));
          }
        }
        hadoopOpts = sb.toString();
      }
      
      // Inherit the environment variables
      String[] env;
      {
        Map<String, String> variables = new HashMap(System.getenv());
        // The user can specify the hadoop memory
        int hadoopMem = conf.getIntVar(HiveConf.ConfVars.HIVEHADOOPMAXMEM);
        
        if (hadoopMem == 0) {
          variables.remove(hadoopMemKey);
        } else {
          // user specified the memory - only applicable for local mode
          variables.put(hadoopMemKey, String.valueOf(hadoopMem));
        }
        
        if (variables.containsKey(hadoopOptsKey)) {
          variables.put(hadoopOptsKey, variables.get(hadoopOptsKey) + hadoopOpts);
        } else {
          variables.put(hadoopOptsKey, hadoopOpts);
        }
        
        env = new String[variables.size()];
        int pos = 0;
        for (Map.Entry<String, String> entry : variables.entrySet()) {  
          String name = entry.getKey();  
          String value = entry.getValue();  
          env[pos++] = name + "=" + value;  
        }  
      }
      
      // Run ExecDriver in another JVM
      executor = Runtime.getRuntime().exec(cmdLine, env);

      StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
      StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
      
      outPrinter.start();
      errPrinter.start();
    
      int exitVal = executor.waitFor();

      if(exitVal != 0) {
        LOG.error("Execution failed with exit status: " + exitVal);
      } else {
        LOG.info("Execution completed successfully");
      }

      return exitVal;
    }
    catch (Exception e) {
      e.printStackTrace();
      LOG.error("Exception: " + e.getMessage());
      return (1);
    }
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public boolean hasReduce() {
    mapredWork w = getWork();
    return w.getReducer() != null;
  }
  
  public int getType() {
    return StageType.MAPREDLOCAL;
  }
}

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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Driver implements CommandProcessor {

  static final private Log LOG = LogFactory.getLog(Driver.class.getName());
  static final private LogHelper console = new LogHelper(LOG);

  private int maxRows = 100;
  ByteStream.Output bos = new ByteStream.Output();

  private HiveConf conf;
  private DataInput resStream;
  private Context ctx;
  private QueryPlan plan;

  public int countJobs(List<Task<? extends Serializable>> tasks) {
    return countJobs(tasks, new ArrayList<Task<? extends Serializable>>());
  }

  public int countJobs(List<Task<? extends Serializable>> tasks, List<Task<? extends Serializable>> seenTasks) {
    if (tasks == null)
      return 0;
    int jobs = 0;
    for (Task<? extends Serializable> task : tasks) {
      if (!seenTasks.contains(task)) {
        seenTasks.add(task);
        if (task.isMapRedTask()) {
          jobs++;
        }
        jobs += countJobs(task.getChildTasks(), seenTasks);
      }
    }
    return jobs;
  }

  /**
   * Return the status information about the Map-Reduce cluster
   */
  public ClusterStatus getClusterStatus() throws Exception {
    ClusterStatus cs;
    try {
      JobConf job = new JobConf(conf, ExecDriver.class);
      JobClient jc = new JobClient(job);
      cs = jc.getClusterStatus();
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning cluster status: " + cs.toString());
    return cs;
  }
  
  /**
   * Get a Schema with fields represented with native Hive types
   */
  public Schema getSchema() throws Exception {
    Schema schema;
    try {
      if (plan != null && plan.getPlan().getFetchTask() != null) {
        BaseSemanticAnalyzer sem = plan.getPlan();

        if (!sem.getFetchTaskInit()) {
          sem.setFetchTaskInit(true);
          sem.getFetchTask().initialize(conf);
        }
        FetchTask ft = (FetchTask) sem.getFetchTask();

        tableDesc td = ft.getTblDesc();
        // partitioned tables don't have tableDesc set on the FetchTask. Instead
        // they have a list of PartitionDesc objects, each with a table desc. Let's
        // try to fetch the desc for the first partition and use it's deserializer.
        if (td == null && ft.getWork() != null && ft.getWork().getPartDesc() != null) {
          if (ft.getWork().getPartDesc().size() > 0) {
            td = ft.getWork().getPartDesc().get(0).getTableDesc();
          }
        }

        if (td == null) {
          throw new Exception("No table description found for fetch task: " + ft);
        }
 
        String tableName = "result";
        List<FieldSchema> lst = MetaStoreUtils.getFieldsFromDeserializer(
            tableName, td.getDeserializer());
        schema = new Schema(lst, null);
      }
      else {
        schema = new Schema();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning Hive schema: " + schema);
    return schema;
  }
  
  /**
   * Get a Schema with fields represented with Thrift DDL types
   */
  public Schema getThriftSchema() throws Exception {
    Schema schema;    
    try {
      schema = this.getSchema();
      if (schema != null) {
	    List<FieldSchema> lst = schema.getFieldSchemas();
	    // Go over the schema and convert type to thrift type
	    if (lst != null) {
	      for (FieldSchema f : lst) {
	        f.setType(MetaStoreUtils.typeToThriftType(f.getType()));  
          }     
	    }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning Thrift schema: " + schema);
    return schema;
  }

  /**
   * Return the maximum number of rows returned by getResults
   */
  public int getMaxRows() {
    return maxRows;
  }

  /**
   * Set the maximum number of rows returned by getResults
   */
  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public boolean hasReduceTasks(List<Task<? extends Serializable>> tasks) {
    if (tasks == null)
      return false;

    boolean hasReduce = false;
    for (Task<? extends Serializable> task : tasks) {
      if (task.hasReduce()) {
        return true;
      }

      hasReduce = (hasReduce || hasReduceTasks(task.getChildTasks()));
    }
    return hasReduce;
  }

  /**
   * for backwards compatibility with current tests
   */
  public Driver(HiveConf conf) {
    this.conf = conf;
  }

  public Driver() {
    if (SessionState.get() != null) {
      conf = SessionState.get().getConf();
    }
  }

  /**
   * Compile a new query. Any currently-planned query associated with this Driver is discarded.
   *
   * @param command The SQL query to compile.
   */
  public int compile(String command) {
    if (plan != null) {
      close();
      plan = null;
    }

    TaskFactory.resetId();

    try {
      ctx = new Context (conf);

      ParseDriver pd = new ParseDriver();
      ASTNode tree = pd.parse(command);

      while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
        tree = (ASTNode) tree.getChild(0);
      }

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
      // Do semantic analysis and plan generation
      sem.analyze(tree, ctx);
      LOG.info("Semantic Analysis Completed");
      
      // validate the plan
      sem.validate();

      plan = new QueryPlan(command, sem);
      return (0);
    } catch (SemanticException e) {
      console.printError("FAILED: Error in semantic analysis: "
          + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (10);
    } catch (ParseException e) {
      console.printError("FAILED: Parse Error: " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (11);
    } catch (Exception e) {
      console.printError("FAILED: Unknown exception : " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (12);
    }
  }

  /**
   * @return The current query plan associated with this Driver, if any.
   */
  public QueryPlan getPlan() {
    return plan;
  }

  public int run(String command) {
    int ret = compile(command);
    if (ret != 0)
      return (ret);

    return execute();
  }

  private List<PreExecute> getPreExecHooks() throws Exception {
    ArrayList<PreExecute> pehooks = new ArrayList<PreExecute>();
    String pestr = conf.getVar(HiveConf.ConfVars.PREEXECHOOKS);
    pestr = pestr.trim();
    if (pestr.equals(""))
      return pehooks;

    String[] peClasses = pestr.split(",");
    
    for(String peClass: peClasses) {
      try {
        pehooks.add((PreExecute)Class.forName(peClass.trim(), true, JavaUtils.getClassLoader()).newInstance());
      } catch (ClassNotFoundException e) {
        console.printError("Pre Exec Hook Class not found:" + e.getMessage());
        throw e;
      }
    }
    
    return pehooks;
  }
  
  public int execute() {
    boolean noName = StringUtils.isEmpty(conf
        .getVar(HiveConf.ConfVars.HADOOPJOBNAME));
    int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);

    String queryId = plan.getQueryId();
    String queryStr = plan.getQueryStr();

    conf.setVar(HiveConf.ConfVars.HIVEQUERYID, queryId);
    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, queryStr);

    try {      
      LOG.info("Starting command: " + queryStr);

      if (SessionState.get() != null)
        SessionState.get().getHiveHistory().startQuery(queryStr, conf.getVar(HiveConf.ConfVars.HIVEQUERYID) );

      resStream = null;

      BaseSemanticAnalyzer sem = plan.getPlan();

      // Get all the pre execution hooks and execute them.
      for(PreExecute peh: getPreExecHooks()) {
        peh.run(SessionState.get(), 
                sem.getInputs(), sem.getOutputs(),
                UserGroupInformation.getCurrentUGI());        
      }
      
      int jobs = countJobs(sem.getRootTasks());
      if (jobs > 0) {
        console.printInfo("Total MapReduce jobs = " + jobs);
      }
      if (SessionState.get() != null){
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_NUM_TASKS, String.valueOf(jobs));
        SessionState.get().getHiveHistory().setIdToTableMap(sem.getIdToTableNameMap());
      }
      String jobname = Utilities.abbreviate(queryStr, maxlen - 6);

      int curJobNo = 0;

      // A very simple runtime that keeps putting runnable tasks on a list and
      // when a job completes, it puts the children at the back of the list
      // while taking the job to run from the front of the list
      Queue<Task<? extends Serializable>> runnable = new LinkedList<Task<? extends Serializable>>();

      for (Task<? extends Serializable> rootTask : sem.getRootTasks()) {
        if (runnable.offer(rootTask) == false) {
          LOG.error("Could not insert the first task into the queue");
          return (1);
        }
      }

      while (runnable.peek() != null) {
        Task<? extends Serializable> tsk = runnable.remove();

        if (SessionState.get() != null)
          SessionState.get().getHiveHistory().startTask(queryId, tsk,
              tsk.getClass().getName());

        if (tsk.isMapRedTask()) {
          curJobNo++;
          if (noName) {
            conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname + "(" + curJobNo
                        + "/" + jobs + ")");
          }
        }

        tsk.initialize(conf);

        int exitVal = tsk.execute();
        if (SessionState.get() != null) {
          SessionState.get().getHiveHistory().setTaskProperty(queryId,
              tsk.getId(), Keys.TASK_RET_CODE, String.valueOf(exitVal));
          SessionState.get().getHiveHistory().endTask(queryId, tsk);
        }
        if (exitVal != 0) {
          console.printError("FAILED: Execution Error, return code " + exitVal
              + " from " + tsk.getClass().getName());
          return 9;
        }
        tsk.setDone();

        if (tsk.getChildTasks() == null) {
          continue;
        }

        for (Task<? extends Serializable> child : tsk.getChildTasks()) {          
          // Check if the child is runnable
          if (!child.isRunnable()) {
            continue;
          }

          if (runnable.offer(child) == false) {
            LOG.error("Could not add child task to queue");
          }
        }
      }
      if (SessionState.get() != null){
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_RET_CODE, String.valueOf(0));
        SessionState.get().getHiveHistory().printRowCount(queryId);
      }
    } catch (Exception e) {
      if (SessionState.get() != null)
        SessionState.get().getHiveHistory().setQueryProperty(queryId,
            Keys.QUERY_RET_CODE, String.valueOf(12));
      console.printError("FAILED: Unknown exception : " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (12);
    } finally {
      if (SessionState.get() != null)
        SessionState.get().getHiveHistory().endQuery(queryId);
      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, "");
      }
    }

    console.printInfo("OK");
    return (0);
  }

  public boolean getResults(Vector<String> res) throws IOException {
    if (plan != null && plan.getPlan().getFetchTask() != null) {
      BaseSemanticAnalyzer sem = plan.getPlan();
      if (!sem.getFetchTaskInit()) {
        sem.setFetchTaskInit(true);
        sem.getFetchTask().initialize(conf);
      }
      FetchTask ft = (FetchTask) sem.getFetchTask();
      ft.setMaxRows(maxRows);
      return ft.fetch(res);
    }

    if (resStream == null)
      resStream = ctx.getStream();
    if (resStream == null)
      return false;

    int numRows = 0;
    String row = null;

    while (numRows < maxRows) {
      if (resStream == null) {
        if (numRows > 0)
          return true;
        else
          return false;
      }

      bos.reset();
      Utilities.streamStatus ss;
      try {
        ss = Utilities.readColumn(resStream, bos);
        if (bos.getCount() > 0)
          row = new String(bos.getData(), 0, bos.getCount(), "UTF-8");
        else if (ss == Utilities.streamStatus.TERMINATED)
          row = new String();

        if (row != null) {
          numRows++;
          res.add(row);
        }
      } catch (IOException e) {
        console.printError("FAILED: Unexpected IO exception : "
            + e.getMessage());
        res = null;
        return false;
      }

      if (ss == Utilities.streamStatus.EOF)
        resStream = ctx.getStream();
    }
    return true;
  }

  public int close() {
    try {
      ctx.clear();
    } catch (Exception e) {
      console.printError("FAILED: Unknown exception : " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return (13);
    }

    return (0);
  }
}

/*
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
package org.apache.hadoop.hive.howl.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.howl.mapreduce.HowlInputFormat.HowlOperation;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.impl.util.ObjectSerializer;

/** The Class which handles checking for features supported by the underlying storage drivers for
 *  the particular Howl query (only partitions matching the filter are checked for).
 */
class HowlFeatureSupport {

    /**
     * Gets the list of features supported for the given partitions. If any one of the underlying InputFormat's does
     * not support the feature and it cannot be implemented by Howl, then the feature is not returned.
     * @param inputInfo  the owl table input info
     * @return the storage features supported for the partitions selected by the setInput call
     */
    public static List<HowlOperation> getSupportedFeatures(JobInfo owlJobInfo) throws Exception {

        List<HowlOperation> owlOperationList = new ArrayList<HowlOperation>();

        List<HowlInputStorageDriver> owlInputStorageDriverList = getUniqueStorageDriver(owlJobInfo/*inputInfo*/);

        // For each HowlOperation, check if the operation is supported for all of the HowlInputStorageDrivers in the above set.
        boolean isSupported;
        for (HowlOperation op: HowlOperation.values()){
            isSupported = true;
            for( HowlInputStorageDriver owlInputStorageDriver : owlInputStorageDriverList){
                try {
                    if (! owlInputStorageDriver.isFeatureSupported(op)) {
                        isSupported = false;
                        break;
                    }
                } catch (IOException e) {
                    throw new Exception("StorageDriver exception",e);
                }
            }
            if (isSupported == true) {
              owlOperationList.add(op);
            }
        }
        return owlOperationList;
    }

    /**
     * Checks if the specified operation is supported for the given partitions. If any one of the underlying InputFormat's does
     * not support the operation and it cannot be implemented by Howl, then returns false. Else returns true.
     * @param inputInfo the owl table input info
     * @param operation the operation to check for
     * @return true, if the feature is supported for selected partitions
     */
    public static boolean isFeatureSupported(JobInfo owlJobInfo,
            HowlOperation operation) throws Exception {

        List<HowlInputStorageDriver> owlInputStorageDriverList = getUniqueStorageDriver(owlJobInfo);

        // For HowlOperation, check if the operation is supported for all of the HowlInputStorageDrivers in the above set.
        for( HowlInputStorageDriver owlInputStorageDriver : owlInputStorageDriverList){
            try {
                if (! owlInputStorageDriver.isFeatureSupported(operation)) {
                    return false;
                }
            } catch (IOException e) {
                throw new Exception("StorageDriver exception",e);
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static List<HowlInputStorageDriver> getUniqueStorageDriver(JobInfo owlJobInfo)
    throws Exception {
        if (owlJobInfo == null){
            throw new Exception("Input jobInfo not initialized");
        }
        List<PartInfo> owlPartitionInfoList = owlJobInfo.getPartitions();
        List<Class<? extends HowlInputStorageDriver>> owlInputStorageDriverClassObjectList
        = new ArrayList<Class<? extends HowlInputStorageDriver>>();

        for (PartInfo owlPartitionInfo:owlPartitionInfoList ){
            String owlInputDriverClassName = owlPartitionInfo.getLoaderInfo().getInputDriverClass();
            Class<? extends HowlInputStorageDriver> owlInputStorageDriverClass;
            try {
                owlInputStorageDriverClass = (Class<? extends HowlInputStorageDriver>)Class.forName(owlInputDriverClassName);
            } catch (ClassNotFoundException e) {
                throw new Exception("Error creating input storage driver instance : " + owlInputDriverClassName, e);
            }
            // create an unique list of HowlInputDriver class objects
            if (! owlInputStorageDriverClassObjectList.contains(owlInputStorageDriverClass)){
                owlInputStorageDriverClassObjectList.add(owlInputStorageDriverClass);
            }
        }
        // create a list instance out of owlInputStorageDriverObjectList
        List<HowlInputStorageDriver> owlInputStorageDriverList = new ArrayList<HowlInputStorageDriver>();
        for (Class<? extends HowlInputStorageDriver> owlInputStorageDriverClassObject : owlInputStorageDriverClassObjectList){
            try {
                HowlInputStorageDriver owlInputStorageDriver = owlInputStorageDriverClassObject.newInstance();
                owlInputStorageDriverList.add(owlInputStorageDriver);
            } catch (Exception e) {
                throw new Exception ("Error creating input storage driver instance : " + owlInputStorageDriverClassObject.getName(), e);
            }
        }
        return owlInputStorageDriverList;
    }

    /**
     * Set the predicate filter for pushdown to the storage driver.
     * @param job the job object
     * @param predicate the predicate filter, an arbitrary AND/OR filter
     * @return true, if the specified predicate can be filtered for the given partitions
     * @throws IOException the exception
     */
    public static boolean setPredicate(Job job, String predicate) throws Exception {
        String jobString = job.getConfiguration().get(HowlInputFormat.HOWL_KEY_JOB_INFO);
        if( jobString == null ) {
            throw new Exception("job not initialized");
        }

        JobInfo owlJobInfo = (JobInfo) ObjectSerializer.deserialize(jobString);

        // First check if storage driver supports predicate pushdown, if not, throw Exception
        if (!isFeatureSupported(/*change to JobInfo*/owlJobInfo, HowlOperation.PREDICATE_PUSHDOWN)){
            throw new IOException("Storage driver doesn't support predicate pushdown.");
        }
        // for each storage driver, check for setPredicate() if any of them is false, return false
        List<HowlInputStorageDriver> owlInputStorageDriverList = getUniqueStorageDriver(owlJobInfo);
        for (HowlInputStorageDriver owlInputStorageDriver : owlInputStorageDriverList){
            // for each storage driver, we need to create a copy of jobcontext to avoid overwriting of the jobcontext by storage driver.
            Job localJob = new Job(job.getConfiguration());
            if (! owlInputStorageDriver.setPredicate(localJob, predicate) ) {
              return false;
            }
        }
        // after making sure every storage driver supports the predicate, finally set predicate into jobcontext
        job.getConfiguration().set(HowlInputFormat.HOWL_KEY_JOB_INFO, predicate);
        return true;
    }

}

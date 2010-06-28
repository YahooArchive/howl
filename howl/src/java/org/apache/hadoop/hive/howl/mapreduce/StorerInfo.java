/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hive.howl.mapreduce;

import java.io.Serializable;

/** Info about the storer to use for writing the data */
public class StorerInfo implements Serializable {

    /** The serialization version */
    private static final long serialVersionUID = 1L;

    /** The class name of the OwlOutputStorageDriver. */
    private String outputDriverClass;

    /** Arguments to be passed to the storage driver. */
    private String outputDriverArgs;

    /** The loader corresponding to this storer. */
    private LoaderInfo loaderInfo;

    /**
     * Instantiates a new owl storer info.
     */
    public StorerInfo() {
    }

    /**
     * Instantiates a new owl storer info.
     *
     * @param outputDriverClass the output driver class
     * @param outputDriverArgs the output driver args
     * @param loaderInfo the loader info
     */
    public StorerInfo(String outputDriverClass, String outputDriverArgs,
            LoaderInfo loaderInfo) {
        this.outputDriverClass = outputDriverClass;
        this.outputDriverArgs = outputDriverArgs;
        this.loaderInfo = loaderInfo;
    }

    /**
     * Gets the output driver class.
     * @return the outputDriverClass
     */
    public String getOutputDriverClass() {
        return outputDriverClass;
    }

    /**
     * Gets the output driver args.
     * @return the outputDriverArgs
     */
    public String getOutputDriverArgs() {
        return outputDriverArgs;
    }

    /**
     * Gets the loader info.
     * @return the loaderInfo
     */
    public LoaderInfo getLoaderInfo() {
        return loaderInfo;
    }

    /**
     * Sets the output driver class.
     * @param outputDriverClass the outputDriverClass to set
     */
    public void setOutputDriverClass(String outputDriverClass) {
        this.outputDriverClass = outputDriverClass;
    }

    /**
     * Sets the output driver args.
     * @param outputDriverArgs the outputDriverArgs to set
     */
    public void setOutputDriverArgs(String outputDriverArgs) {
        this.outputDriverArgs = outputDriverArgs;
    }

    /**
     * Sets the loader info.
     * @param loaderInfo the loaderInfo to set
     */
    public void setLoaderInfo(LoaderInfo loaderInfo) {
        this.loaderInfo = loaderInfo;
    }

}

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
package org.apache.hadoop.hive.howl.common;

/**
 * Enum type representing the various errors throws by Howl.
 */
public enum ErrorType {

    /* Howl Input Format related errors 1000 - 1999 */
    ERROR_DB_INIT                       (1000, "Error initializing database session"),


    /* Howl Output Format related errors 2000 - 2999 */
    ERROR_INVALID_TABLE                 (2001, "Table specified does not exist"),


    /* Miscellaneous errors, range 9000 - 9998 */
    ERROR_UNIMPLEMENTED                 (9000, "Functionality currently unimplemented"),
    ERROR_INTERNAL_EXCEPTION            (9001, "Exception occurred while processing Howl request");

    /** The error code. */
    private int errorCode;

    /** The error message. */
    private String errorMessage;

    /** Should the causal exception message be appended to the error message, yes by default*/
    private boolean appendCauseMessage = true;

    /** Is this a retriable error, no by default. */
    private boolean isRetriable = false;

    /**
     * Instantiates a new error type.
     * @param errorCode the error code
     * @param errorMessage the error message
     */
    private ErrorType(int errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    /**
     * Instantiates a new error type.
     * @param errorCode the error code
     * @param errorMessage the error message
     * @param appendCauseMessage should causal exception message be appended to error message
     */
    private ErrorType(int errorCode, String errorMessage, boolean appendCauseMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.appendCauseMessage = appendCauseMessage;
    }

    /**
     * Instantiates a new error type.
     * @param errorCode the error code
     * @param errorMessage the error message
     * @param appendCauseMessage should causal exception message be appended to error message
     * @param isRetriable is this a retriable error
     */
    private ErrorType(int errorCode, String errorMessage, boolean appendCauseMessage, boolean isRetriable) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.appendCauseMessage = appendCauseMessage;
        this.isRetriable = isRetriable;
    }

    /**
     * Gets the error code.
     * @return the error code
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Gets the error message.
     * @return the error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Checks if this is a retriable error.
     * @return true, if is a retriable error, false otherwise
     */
    public boolean isRetriable() {
        return isRetriable;
    }

    /**
     * Whether the cause of the exception should be added to the error message.
     * @return true, if the cause should be added to the message, false otherwise
     */
    public boolean appendCauseMessage() {
        return appendCauseMessage;
    }
}

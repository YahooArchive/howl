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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;

/**
 * DDLWork.
 *
 */
public class DDLWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private CreateIndexDesc createIndexDesc;
  private DropIndexDesc dropIdxDesc;
  private CreateDatabaseDesc createDatabaseDesc;
  private SwitchDatabaseDesc switchDatabaseDesc;
  private DropDatabaseDesc dropDatabaseDesc;
  private CreateTableDesc createTblDesc;
  private CreateTableLikeDesc createTblLikeDesc;
  private CreateViewDesc createVwDesc;
  private DropTableDesc dropTblDesc;
  private AlterTableDesc alterTblDesc;
  private ShowDatabasesDesc showDatabasesDesc;
  private ShowTablesDesc showTblsDesc;
  private LockTableDesc lockTblDesc;
  private UnlockTableDesc unlockTblDesc;
  private ShowFunctionsDesc showFuncsDesc;
  private ShowLocksDesc showLocksDesc;
  private DescFunctionDesc descFunctionDesc;
  private ShowPartitionsDesc showPartsDesc;
  private DescTableDesc descTblDesc;
  private AddPartitionDesc addPartitionDesc;
  private AlterTableSimpleDesc alterTblSimpleDesc;
  private MsckDesc msckDesc;
  private ShowTableStatusDesc showTblStatusDesc;

  /**
   * ReadEntitites that are passed to the hooks.
   */
  protected HashSet<ReadEntity> inputs;
  /**
   * List of WriteEntities that are passed to the hooks.
   */
  protected HashSet<WriteEntity> outputs;

  public DDLWork() {
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public DDLWork(CreateIndexDesc createIndex) {
    this.createIndexDesc = createIndex;
  }
  
  /**
   * @param createDatabaseDesc
   *          Create Database descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateDatabaseDesc createDatabaseDesc) {
    this(inputs, outputs);
    this.createDatabaseDesc = createDatabaseDesc;
  }

  /**
   * @param dropDatabaseDesc
   *          Drop Database descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DropDatabaseDesc dropDatabaseDesc) {
    this(inputs, outputs);
    this.dropDatabaseDesc = dropDatabaseDesc;
  }

  /**
   * @param switchDatabaseDesc
   *          Switch Database descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      SwitchDatabaseDesc switchDatabaseDesc) {
    this(inputs, outputs);
    this.switchDatabaseDesc = switchDatabaseDesc;
  }

  /**
   * @param alterTblDesc
   *          alter table descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterTableDesc alterTblDesc) {
    this(inputs, outputs);
    this.alterTblDesc = alterTblDesc;
  }

  /**
   * @param createTblDesc
   *          create table descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateTableDesc createTblDesc) {
    this(inputs, outputs);

    this.createTblDesc = createTblDesc;
  }

  /**
   * @param createTblLikeDesc
   *          create table like descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateTableLikeDesc createTblLikeDesc) {
    this(inputs, outputs);

    this.createTblLikeDesc = createTblLikeDesc;
  }

  /**
   * @param createVwDesc
   *          create view descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      CreateViewDesc createVwDesc) {
    this(inputs, outputs);

    this.createVwDesc = createVwDesc;
  }

  /**
   * @param dropTblDesc
   *          drop table descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DropTableDesc dropTblDesc) {
    this(inputs, outputs);

    this.dropTblDesc = dropTblDesc;
  }

  /**
   * @param descTblDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DescTableDesc descTblDesc) {
    this(inputs, outputs);

    this.descTblDesc = descTblDesc;
  }

  /**
   * @param showDatabasesDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowDatabasesDesc showDatabasesDesc) {
    this(inputs, outputs);

    this.showDatabasesDesc = showDatabasesDesc;
  }

  /**
   * @param showTblsDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowTablesDesc showTblsDesc) {
    this(inputs, outputs);

    this.showTblsDesc = showTblsDesc;
  }

  /**
   * @param lockTblDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      LockTableDesc lockTblDesc) {
    this(inputs, outputs);

    this.lockTblDesc = lockTblDesc;
  }

  /**
   * @param unlockTblDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      UnlockTableDesc unlockTblDesc) {
    this(inputs, outputs);

    this.unlockTblDesc = unlockTblDesc;
  }

  /**
   * @param showFuncsDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowFunctionsDesc showFuncsDesc) {
    this(inputs, outputs);

    this.showFuncsDesc = showFuncsDesc;
  }

  /**
   * @param showLocksDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowLocksDesc showLocksDesc) {
    this(inputs, outputs);

    this.showLocksDesc = showLocksDesc;
  }

  /**
   * @param descFuncDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DescFunctionDesc descFuncDesc) {
    this(inputs, outputs);

    descFunctionDesc = descFuncDesc;
  }

  /**
   * @param showPartsDesc
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowPartitionsDesc showPartsDesc) {
    this(inputs, outputs);

    this.showPartsDesc = showPartsDesc;
  }

  /**
   * @param addPartitionDesc
   *          information about the partitions we want to add.
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AddPartitionDesc addPartitionDesc) {
    this(inputs, outputs);

    this.addPartitionDesc = addPartitionDesc;
  }

  /**
   * @param touchDesc
   *          information about the table/partitions that we want to touch
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      AlterTableSimpleDesc simpleDesc) {
    this(inputs, outputs);

    this.alterTblSimpleDesc = simpleDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      MsckDesc checkDesc) {
    this(inputs, outputs);

    msckDesc = checkDesc;
  }

  /**
   * @param showTblStatusDesc
   *          show table status descriptor
   */
  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      ShowTableStatusDesc showTblStatusDesc) {
    this(inputs, outputs);

    this.showTblStatusDesc = showTblStatusDesc;
  }

  public DDLWork(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      DropIndexDesc dropIndexDesc) {
    this(inputs, outputs);
    this.dropIdxDesc = dropIndexDesc;
  }

  /**
   * @return Create Database descriptor
   */
  public CreateDatabaseDesc getCreateDatabaseDesc() {
    return createDatabaseDesc;
  }

  /**
   * Set Create Database descriptor
   * @param createDatabaseDesc
   */
  public void setCreateDatabaseDesc(CreateDatabaseDesc createDatabaseDesc) {
    this.createDatabaseDesc = createDatabaseDesc;
  }

  /**
   * @return Drop Database descriptor
   */
  public DropDatabaseDesc getDropDatabaseDesc() {
    return dropDatabaseDesc;
  }

  /**
   * Set Drop Database descriptor
   * @param dropDatabaseDesc
   */
  public void setDropDatabaseDesc(DropDatabaseDesc dropDatabaseDesc) {
    this.dropDatabaseDesc = dropDatabaseDesc;
  }

  /**
   * @return Switch Database descriptor
   */
  public SwitchDatabaseDesc getSwitchDatabaseDesc() {
    return switchDatabaseDesc;
  }

  /**
   * Set Switch Database descriptor
   * @param switchDatabaseDesc
   */
  public void setSwitchDatabaseDesc(SwitchDatabaseDesc switchDatabaseDesc) {
    this.switchDatabaseDesc = switchDatabaseDesc;
  }

  /**
   * @return the createTblDesc
   */
  @Explain(displayName = "Create Table Operator")
  public CreateTableDesc getCreateTblDesc() {
    return createTblDesc;
  }

  /**
   * @param createTblDesc
   *          the createTblDesc to set
   */
  public void setCreateTblDesc(CreateTableDesc createTblDesc) {
    this.createTblDesc = createTblDesc;
  }
  
  public CreateIndexDesc getCreateIndexDesc() {
    return createIndexDesc;
  }

  public void setCreateIndexDesc(CreateIndexDesc createIndexDesc) {
    this.createIndexDesc = createIndexDesc;
  }

  /**
   * @return the createTblDesc
   */
  @Explain(displayName = "Create Table Operator")
  public CreateTableLikeDesc getCreateTblLikeDesc() {
    return createTblLikeDesc;
  }

  /**
   * @param createTblLikeDesc
   *          the createTblDesc to set
   */
  public void setCreateTblLikeDesc(CreateTableLikeDesc createTblLikeDesc) {
    this.createTblLikeDesc = createTblLikeDesc;
  }

  /**
   * @return the createTblDesc
   */
  @Explain(displayName = "Create View Operator")
  public CreateViewDesc getCreateViewDesc() {
    return createVwDesc;
  }

  /**
   * @param createVwDesc
   *          the createViewDesc to set
   */
  public void setCreateViewDesc(CreateViewDesc createVwDesc) {
    this.createVwDesc = createVwDesc;
  }

  /**
   * @return the dropTblDesc
   */
  @Explain(displayName = "Drop Table Operator")
  public DropTableDesc getDropTblDesc() {
    return dropTblDesc;
  }

  /**
   * @param dropTblDesc
   *          the dropTblDesc to set
   */
  public void setDropTblDesc(DropTableDesc dropTblDesc) {
    this.dropTblDesc = dropTblDesc;
  }

  /**
   * @return the alterTblDesc
   */
  @Explain(displayName = "Alter Table Operator")
  public AlterTableDesc getAlterTblDesc() {
    return alterTblDesc;
  }

  /**
   * @param alterTblDesc
   *          the alterTblDesc to set
   */
  public void setAlterTblDesc(AlterTableDesc alterTblDesc) {
    this.alterTblDesc = alterTblDesc;
  }

  /**
   * @return the showDatabasesDesc
   */
  @Explain(displayName = "Show Databases Operator")
  public ShowDatabasesDesc getShowDatabasesDesc() {
    return showDatabasesDesc;
  }

  /**
   * @param showDatabasesDesc
   *          the showDatabasesDesc to set
   */
  public void setShowDatabasesDesc(ShowDatabasesDesc showDatabasesDesc) {
    this.showDatabasesDesc = showDatabasesDesc;
  }

  /**
   * @return the showTblsDesc
   */
  @Explain(displayName = "Show Table Operator")
  public ShowTablesDesc getShowTblsDesc() {
    return showTblsDesc;
  }

  /**
   * @param showTblsDesc
   *          the showTblsDesc to set
   */
  public void setShowTblsDesc(ShowTablesDesc showTblsDesc) {
    this.showTblsDesc = showTblsDesc;
  }

  /**
   * @return the showFuncsDesc
   */
  @Explain(displayName = "Show Function Operator")
  public ShowFunctionsDesc getShowFuncsDesc() {
    return showFuncsDesc;
  }

  /**
   * @return the showLocksDesc
   */
  @Explain(displayName = "Show Lock Operator")
  public ShowLocksDesc getShowLocksDesc() {
    return showLocksDesc;
  }

  /**
   * @return the lockTblDesc
   */
  @Explain(displayName = "Lock Table Operator")
  public LockTableDesc getLockTblDesc() {
    return lockTblDesc;
  }

  /**
   * @return the unlockTblDesc
   */
  @Explain(displayName = "Unlock Table Operator")
  public UnlockTableDesc getUnlockTblDesc() {
    return unlockTblDesc;
  }

  /**
   * @return the descFuncDesc
   */
  @Explain(displayName = "Show Function Operator")
  public DescFunctionDesc getDescFunctionDesc() {
    return descFunctionDesc;
  }

  /**
   * @param showFuncsDesc
   *          the showFuncsDesc to set
   */
  public void setShowFuncsDesc(ShowFunctionsDesc showFuncsDesc) {
    this.showFuncsDesc = showFuncsDesc;
  }

  /**
   * @param showLocksDesc
   *          the showLocksDesc to set
   */
  public void setShowLocksDesc(ShowLocksDesc showLocksDesc) {
    this.showLocksDesc = showLocksDesc;
  }

  /**
   * @param lockTblDesc
   *          the lockTblDesc to set
   */
  public void setLockTblDesc(LockTableDesc lockTblDesc) {
    this.lockTblDesc = lockTblDesc;
  }

  /**
   * @param unlockTblDesc
   *          the unlockTblDesc to set
   */
  public void setUnlockTblDesc(UnlockTableDesc unlockTblDesc) {
    this.unlockTblDesc = unlockTblDesc;
  }

  /**
   * @param descFuncDesc
   *          the showFuncsDesc to set
   */
  public void setDescFuncDesc(DescFunctionDesc descFuncDesc) {
    descFunctionDesc = descFuncDesc;
  }

  /**
   * @return the showPartsDesc
   */
  @Explain(displayName = "Show Partitions Operator")
  public ShowPartitionsDesc getShowPartsDesc() {
    return showPartsDesc;
  }

  /**
   * @param showPartsDesc
   *          the showPartsDesc to set
   */
  public void setShowPartsDesc(ShowPartitionsDesc showPartsDesc) {
    this.showPartsDesc = showPartsDesc;
  }

  /**
   * @return the descTblDesc
   */
  @Explain(displayName = "Describe Table Operator")
  public DescTableDesc getDescTblDesc() {
    return descTblDesc;
  }

  /**
   * @param descTblDesc
   *          the descTblDesc to set
   */
  public void setDescTblDesc(DescTableDesc descTblDesc) {
    this.descTblDesc = descTblDesc;
  }

  /**
   * @return information about the partitions we want to add.
   */
  public AddPartitionDesc getAddPartitionDesc() {
    return addPartitionDesc;
  }

  /**
   * @param addPartitionDesc
   *          information about the partitions we want to add.
   */
  public void setAddPartitionDesc(AddPartitionDesc addPartitionDesc) {
    this.addPartitionDesc = addPartitionDesc;
  }

  /**
   * @return information about the table/partitions we want to alter.
   */
  public AlterTableSimpleDesc getAlterTblSimpleDesc() {
    return alterTblSimpleDesc;
  }

  /**
   * @param desc
   *          information about the table/partitions we want to alter.
   */
  public void setAlterTblSimpleDesc(AlterTableSimpleDesc desc) {
    this.alterTblSimpleDesc = desc;
  }

  /**
   * @return Metastore check description
   */
  public MsckDesc getMsckDesc() {
    return msckDesc;
  }

  /**
   * @param msckDesc
   *          metastore check description
   */
  public void setMsckDesc(MsckDesc msckDesc) {
    this.msckDesc = msckDesc;
  }

  /**
   * @return show table descriptor
   */
  public ShowTableStatusDesc getShowTblStatusDesc() {
    return showTblStatusDesc;
  }

  /**
   * @param showTblStatusDesc
   *          show table descriptor
   */
  public void setShowTblStatusDesc(ShowTableStatusDesc showTblStatusDesc) {
    this.showTblStatusDesc = showTblStatusDesc;
  }

  public CreateViewDesc getCreateVwDesc() {
    return createVwDesc;
  }

  public void setCreateVwDesc(CreateViewDesc createVwDesc) {
    this.createVwDesc = createVwDesc;
  }

  public void setDescFunctionDesc(DescFunctionDesc descFunctionDesc) {
    this.descFunctionDesc = descFunctionDesc;
  }

  public HashSet<ReadEntity> getInputs() {
    return inputs;
  }

  public HashSet<WriteEntity> getOutputs() {
    return outputs;
  }

  public void setInputs(HashSet<ReadEntity> inputs) {
    this.inputs = inputs;
  }

  public void setOutputs(HashSet<WriteEntity> outputs) {
    this.outputs = outputs;
  }
  
  public DropIndexDesc getDropIdxDesc() {
    return dropIdxDesc;
  }

  public void setDropIdxDesc(DropIndexDesc dropIdxDesc) {
    this.dropIdxDesc = dropIdxDesc;
  }

}

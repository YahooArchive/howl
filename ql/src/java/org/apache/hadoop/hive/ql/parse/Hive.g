grammar Hive;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}

tokens {
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_SELECTDI;
TOK_SELEXPR;
TOK_FROM;
TOK_TAB;
TOK_PARTSPEC;
TOK_PARTVAL;
TOK_DIR;
TOK_LOCAL_DIR;
TOK_TABREF;
TOK_SUBQUERY;
TOK_DESTINATION;
TOK_ALLCOLREF;
TOK_TABLE_OR_COL;
TOK_FUNCTION;
TOK_FUNCTIONDI;
TOK_FUNCTIONSTAR;
TOK_WHERE;
TOK_OP_EQ;
TOK_OP_NE;
TOK_OP_LE;
TOK_OP_LT;
TOK_OP_GE;
TOK_OP_GT;
TOK_OP_DIV;
TOK_OP_ADD;
TOK_OP_SUB;
TOK_OP_MUL;
TOK_OP_MOD;
TOK_OP_BITAND;
TOK_OP_BITNOT;
TOK_OP_BITOR;
TOK_OP_BITXOR;
TOK_OP_AND;
TOK_OP_OR;
TOK_OP_NOT;
TOK_OP_LIKE;
TOK_TRUE;
TOK_FALSE;
TOK_TRANSFORM;
TOK_SERDE;
TOK_SERDENAME;
TOK_SERDEPROPS;
TOK_EXPLIST;
TOK_ALIASLIST;
TOK_GROUPBY;
TOK_ORDERBY;
TOK_CLUSTERBY;
TOK_DISTRIBUTEBY;
TOK_SORTBY;
TOK_UNION;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_UNIQUEJOIN;
TOK_LOAD;
TOK_NULL;
TOK_ISNULL;
TOK_ISNOTNULL;
TOK_TINYINT;
TOK_SMALLINT;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FLOAT;
TOK_DOUBLE;
TOK_DATE;
TOK_DATETIME;
TOK_TIMESTAMP;
TOK_STRING;
TOK_LIST;
TOK_STRUCT;
TOK_MAP;
TOK_CREATEDATABASE;
TOK_CREATETABLE;
TOK_CREATEINDEX;
TOK_CREATEINDEX_INDEXTBLNAME;
TOK_DEFERRED_REBUILDINDEX;
TOK_DROPINDEX;
TOK_LIKETABLE;
TOK_DESCTABLE;
TOK_DESCFUNCTION;
TOK_ALTERTABLE_PARTITION;
TOK_ALTERTABLE_RENAME;
TOK_ALTERTABLE_ADDCOLS;
TOK_ALTERTABLE_RENAMECOL;
TOK_ALTERTABLE_REPLACECOLS;
TOK_ALTERTABLE_ADDPARTS;
TOK_ALTERTABLE_DROPPARTS;
TOK_ALTERTABLE_ALTERPARTS_PROTECTMODE;
TOK_ALTERTABLE_TOUCH;
TOK_ALTERTABLE_ARCHIVE;
TOK_ALTERTABLE_UNARCHIVE;
TOK_ALTERTABLE_SERDEPROPERTIES;
TOK_ALTERTABLE_SERIALIZER;
TOK_TABLE_PARTITION;
TOK_ALTERTABLE_FILEFORMAT;
TOK_ALTERTABLE_LOCATION;
TOK_ALTERTABLE_PROPERTIES;
TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION;
TOK_ALTERINDEX_REBUILD;
TOK_MSCK;
TOK_SHOWDATABASES;
TOK_SHOWTABLES;
TOK_SHOWFUNCTIONS;
TOK_SHOWPARTITIONS;
TOK_SHOW_TABLESTATUS;
TOK_SHOWLOCKS;
TOK_LOCKTABLE;
TOK_UNLOCKTABLE;
TOK_SWITCHDATABASE;
TOK_DROPDATABASE;
TOK_DROPTABLE;
TOK_DATABASECOMMENT;
TOK_TABCOLLIST;
TOK_TABCOL;
TOK_TABLECOMMENT;
TOK_TABLEPARTCOLS;
TOK_TABLEBUCKETS;
TOK_TABLEROWFORMAT;
TOK_TABLEROWFORMATFIELD;
TOK_TABLEROWFORMATCOLLITEMS;
TOK_TABLEROWFORMATMAPKEYS;
TOK_TABLEROWFORMATLINES;
TOK_TBLSEQUENCEFILE;
TOK_TBLTEXTFILE;
TOK_TBLRCFILE;
TOK_TABLEFILEFORMAT;
TOK_OFFLINE;
TOK_ENABLE;
TOK_DISABLE;
TOK_READONLY;
TOK_NO_DROP;
TOK_STORAGEHANDLER;
TOK_ALTERTABLE_CLUSTER_SORT;
TOK_TABCOLNAME;
TOK_TABLELOCATION;
TOK_PARTITIONLOCATION;
TOK_TABLESAMPLE;
TOK_TMP_FILE;
TOK_TABSORTCOLNAMEASC;
TOK_TABSORTCOLNAMEDESC;
TOK_CHARSETLITERAL;
TOK_CREATEFUNCTION;
TOK_DROPFUNCTION;
TOK_CREATEVIEW;
TOK_DROPVIEW;
TOK_ALTERVIEW_PROPERTIES;
TOK_EXPLAIN;
TOK_TABLESERIALIZER;
TOK_TABLEPROPERTIES;
TOK_TABLEPROPLIST;
TOK_TABTYPE;
TOK_LIMIT;
TOK_TABLEPROPERTY;
TOK_IFEXISTS;
TOK_IFNOTEXISTS;
TOK_HINTLIST;
TOK_HINT;
TOK_MAPJOIN;
TOK_STREAMTABLE;
TOK_HOLD_DDLTIME;
TOK_HINTARGLIST;
TOK_USERSCRIPTCOLNAMES;
TOK_USERSCRIPTCOLSCHEMA;
TOK_RECORDREADER;
TOK_RECORDWRITER;
TOK_LEFTSEMIJOIN;
TOK_LATERAL_VIEW;
TOK_TABALIAS;
TOK_ANALYZE;
}


// Package headers
@header {
package org.apache.hadoop.hive.ql.parse;
}
@lexer::header {package org.apache.hadoop.hive.ql.parse;}


@members {
  Stack msgs = new Stack<String>();
}

@rulecatch {
catch (RecognitionException e) {
 reportError(e);
  throw e;
}
}

// starting rule
statement
	: explainStatement EOF
	| execStatement EOF
	;

explainStatement
@init { msgs.push("explain statement"); }
@after { msgs.pop(); }
	: KW_EXPLAIN (isExtended=KW_EXTENDED)? execStatement -> ^(TOK_EXPLAIN execStatement $isExtended?)
	;

execStatement
@init { msgs.push("statement"); }
@after { msgs.pop(); }
    : queryStatementExpression
    | loadStatement
    | ddlStatement
    ;

loadStatement
@init { msgs.push("load statement"); }
@after { msgs.pop(); }
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tabName)
    -> ^(TOK_LOAD $path $tab $islocal? $isoverwrite?)
    ;

ddlStatement
@init { msgs.push("ddl statement"); }
@after { msgs.pop(); }
    : createDatabaseStatement
    | switchDatabaseStatement
    | dropDatabaseStatement
    | createTableStatement
    | dropTableStatement
    | alterStatement
    | descStatement
    | showStatement
    | metastoreCheck
    | createViewStatement
    | dropViewStatement
    | createFunctionStatement
    | createIndexStatement
    | dropIndexStatement
    | alterIndexRebuild
    | dropFunctionStatement
    | analyzeStatement
    | lockStatement
    | unlockStatement
    ;

ifExists
@init { msgs.push("if exists clause"); }
@after { msgs.pop(); }
    : KW_IF KW_EXISTS
    -> ^(TOK_IFEXISTS)
    ;

ifNotExists
@init { msgs.push("if not exists clause"); }
@after { msgs.pop(); }
    : KW_IF KW_NOT KW_EXISTS
    -> ^(TOK_IFNOTEXISTS)
    ;


createDatabaseStatement
@init { msgs.push("create database statement"); }
@after { msgs.pop(); }
    : KW_CREATE (KW_DATABASE|KW_SCHEMA)
        ifNotExists?
        name=Identifier
        databaseComment?
    -> ^(TOK_CREATEDATABASE $name ifNotExists? databaseComment?)
    ;

switchDatabaseStatement
@init { msgs.push("switch database statement"); }
@after { msgs.pop(); }
    : KW_USE Identifier
    -> ^(TOK_SWITCHDATABASE Identifier)
    ;

dropDatabaseStatement
@init { msgs.push("drop database statement"); }
@after { msgs.pop(); }
    : KW_DROP (KW_DATABASE|KW_SCHEMA) ifExists? Identifier
    -> ^(TOK_DROPDATABASE Identifier ifExists?)
    ;

databaseComment
@init { msgs.push("database's comment"); }
@after { msgs.pop(); }
    : KW_COMMENT comment=StringLiteral
    -> ^(TOK_DATABASECOMMENT $comment)
    ;

createTableStatement
@init { msgs.push("create table statement"); }
@after { msgs.pop(); }
    : KW_CREATE (ext=KW_EXTERNAL)? KW_TABLE ifNotExists? name=Identifier
      (  like=KW_LIKE likeName=Identifier
         tableLocation?
       | (LPAREN columnNameTypeList RPAREN)?
         tableComment?
         tablePartition?
         tableBuckets?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         (KW_AS selectStatement)?
      )
    -> ^(TOK_CREATETABLE $name $ext? ifNotExists?
         ^(TOK_LIKETABLE $likeName?)
         columnNameTypeList?
         tableComment?
         tablePartition?
         tableBuckets?
         tableRowFormat?
         tableFileFormat?
         tableLocation?
         tablePropertiesPrefixed?
         selectStatement?
        )
    ;

createIndexStatement
@init { msgs.push("create index statement");}
@after {msgs.pop();}
    : KW_CREATE KW_INDEX indexName=Identifier 
      KW_ON KW_TABLE tab=Identifier LPAREN indexedCols=columnNameList RPAREN 
      KW_AS typeName=StringLiteral
      autoRebuild?
      indexTblName?
      tableRowFormat?
      tableFileFormat?
      tableLocation?
    ->^(TOK_CREATEINDEX $indexName $typeName $tab $indexedCols 
        autoRebuild?
        indexTblName?
        tableRowFormat?
        tableFileFormat?
        tableLocation?)
    ;

autoRebuild
@init { msgs.push("auto rebuild index");}
@after {msgs.pop();}
    : KW_WITH KW_DEFERRED KW_REBUILD
    ->^(TOK_DEFERRED_REBUILDINDEX)
    ;

indexTblName
@init { msgs.push("index table name");}
@after {msgs.pop();}
    : KW_IN KW_TABLE indexTbl=Identifier
    ->^(TOK_CREATEINDEX_INDEXTBLNAME $indexTbl)
    ;

indexPropertiesPrefixed
@init { msgs.push("table properties with prefix"); }
@after { msgs.pop(); }
    :
        KW_IDXPROPERTIES! indexProperties
    ;

indexProperties
@init { msgs.push("table properties"); }
@after { msgs.pop(); }
    :
      LPAREN propertiesList RPAREN -> ^(TOK_TABLEPROPERTIES propertiesList)
    ;

dropIndexStatement
@init { msgs.push("drop index statement");}
@after {msgs.pop();}
    : KW_DROP KW_INDEX indexName=Identifier KW_ON tab=Identifier
    ->^(TOK_DROPINDEX $indexName $tab)
    ;

dropTableStatement
@init { msgs.push("drop statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_TABLE Identifier  -> ^(TOK_DROPTABLE Identifier)
    ;

alterStatement
@init { msgs.push("alter statement"); }
@after { msgs.pop(); }
    : KW_ALTER!
        (
            KW_TABLE! alterTableStatementSuffix
        |
            KW_VIEW! alterViewStatementSuffix
        )
    ;

alterTableStatementSuffix
@init { msgs.push("alter table statement"); }
@after { msgs.pop(); }
    : alterStatementSuffixRename
    | alterStatementSuffixAddCol
    | alterStatementSuffixRenameCol
    | alterStatementSuffixDropPartitions
    | alterStatementSuffixAddPartitions
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | alterStatementSuffixSerdeProperties
    | alterTblPartitionStatement
    | alterStatementSuffixClusterbySortby
    ;

alterViewStatementSuffix
@init { msgs.push("alter view statement"); }
@after { msgs.pop(); }
    : alterViewSuffixProperties
    ;

alterStatementSuffixRename
@init { msgs.push("rename statement"); }
@after { msgs.pop(); }
    : oldName=Identifier KW_RENAME KW_TO newName=Identifier
    -> ^(TOK_ALTERTABLE_RENAME $oldName $newName)
    ;

alterStatementSuffixAddCol
@init { msgs.push("add column statement"); }
@after { msgs.pop(); }
    : Identifier (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN
    -> {$add != null}? ^(TOK_ALTERTABLE_ADDCOLS Identifier columnNameTypeList)
    ->                 ^(TOK_ALTERTABLE_REPLACECOLS Identifier columnNameTypeList)
    ;

alterStatementSuffixRenameCol
@init { msgs.push("rename column name"); }
@after { msgs.pop(); }
    : Identifier KW_CHANGE KW_COLUMN? oldName=Identifier newName=Identifier colType (KW_COMMENT comment=StringLiteral)? alterStatementChangeColPosition?
    ->^(TOK_ALTERTABLE_RENAMECOL Identifier $oldName $newName colType $comment? alterStatementChangeColPosition?)
    ;

alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=Identifier
    ->{$first != null}? ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION )
    -> ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION $afterCol)
    ;

alterStatementSuffixAddPartitions
@init { msgs.push("add partition statement"); }
@after { msgs.pop(); }
    : Identifier KW_ADD ifNotExists? partitionSpec partitionLocation? (partitionSpec partitionLocation?)*
    -> ^(TOK_ALTERTABLE_ADDPARTS Identifier ifNotExists? (partitionSpec partitionLocation?)+)
    ;
    
alterStatementSuffixTouch
@init { msgs.push("touch statement"); }
@after { msgs.pop(); }
    : Identifier KW_TOUCH (partitionSpec)*
    -> ^(TOK_ALTERTABLE_TOUCH Identifier (partitionSpec)*)
    ;

alterStatementSuffixArchive
@init { msgs.push("archive statement"); }
@after { msgs.pop(); }
    : Identifier KW_ARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_ARCHIVE Identifier (partitionSpec)*)
    ;

alterStatementSuffixUnArchive
@init { msgs.push("unarchive statement"); }
@after { msgs.pop(); }
    : Identifier KW_UNARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_UNARCHIVE Identifier (partitionSpec)*)
    ;

partitionLocation
@init { msgs.push("partition location"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_PARTITIONLOCATION $locn)
    ;

alterStatementSuffixDropPartitions
@init { msgs.push("drop partition statement"); }
@after { msgs.pop(); }
    : Identifier KW_DROP partitionSpec (COMMA partitionSpec)*
    -> ^(TOK_ALTERTABLE_DROPPARTS Identifier partitionSpec+)
    ;

alterStatementSuffixProperties
@init { msgs.push("alter properties statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_PROPERTIES $name tableProperties)
    ;

alterViewSuffixProperties
@init { msgs.push("alter view properties statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERVIEW_PROPERTIES $name tableProperties)
    ;

alterStatementSuffixSerdeProperties
@init { msgs.push("alter serdes statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_SERDE serdeName=StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    -> ^(TOK_ALTERTABLE_SERIALIZER $name $serdeName tableProperties?)
    | name=Identifier KW_SET KW_SERDEPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_SERDEPROPERTIES $name tableProperties)
    ;

tablePartitionPrefix
@init {msgs.push("table partition prefix");}
@after {msgs.pop();}
  :name=Identifier partitionSpec? 
  ->^(TOK_TABLE_PARTITION $name partitionSpec?)
  ;

alterTblPartitionStatement
@init {msgs.push("alter table partition statement");}
@after {msgs.pop();}
  :  tablePartitionPrefix alterTblPartitionStatementSuffix
  -> ^(TOK_ALTERTABLE_PARTITION tablePartitionPrefix alterTblPartitionStatementSuffix)
  ;

alterTblPartitionStatementSuffix
@init {msgs.push("alter table partition statement suffix");}
@after {msgs.pop();}
  : alterStatementSuffixFileFormat
  | alterStatementSuffixLocation
  | alterStatementSuffixProtectMode
  ;

alterStatementSuffixFileFormat
@init {msgs.push("alter fileformat statement"); }
@after {msgs.pop();}
	: KW_SET KW_FILEFORMAT fileFormat
	-> ^(TOK_ALTERTABLE_FILEFORMAT fileFormat)
	;

alterStatementSuffixLocation
@init {msgs.push("alter location");}
@after {msgs.pop();}
  : KW_SET KW_LOCATION newLoc=StringLiteral
  -> ^(TOK_ALTERTABLE_LOCATION $newLoc)
  ;

alterStatementSuffixProtectMode
@init { msgs.push("alter partition protect mode statement"); }
@after { msgs.pop(); }
    : alterProtectMode
    -> ^(TOK_ALTERTABLE_ALTERPARTS_PROTECTMODE alterProtectMode)
    ;

alterProtectMode
@init { msgs.push("protect mode specification enable"); }
@after { msgs.pop(); }
    : KW_ENABLE alterProtectModeMode  -> ^(TOK_ENABLE alterProtectModeMode)
    | KW_DISABLE alterProtectModeMode  -> ^(TOK_DISABLE alterProtectModeMode)
    ;

alterProtectModeMode
@init { msgs.push("protect mode specification enable"); }
@after { msgs.pop(); }
    : KW_OFFLINE  -> ^(TOK_OFFLINE)
    | KW_NO_DROP  -> ^(TOK_NO_DROP)
    | KW_READONLY  -> ^(TOK_READONLY)
    ;


alterStatementSuffixClusterbySortby
@init {msgs.push("alter cluster by sort by statement");}
@after{msgs.pop();}
	:name=Identifier tableBuckets
	->^(TOK_ALTERTABLE_CLUSTER_SORT $name tableBuckets)
	| 
	name=Identifier KW_NOT KW_CLUSTERED
	->^(TOK_ALTERTABLE_CLUSTER_SORT $name)
	;

alterIndexRebuild
@init { msgs.push("update index statement");}
@after {msgs.pop();}
    : KW_ALTER KW_INDEX indexName=Identifier KW_ON base_table_name=Identifier partitionSpec? KW_REBUILD
    ->^(TOK_ALTERINDEX_REBUILD $base_table_name $indexName partitionSpec?)
    ;

fileFormat
@init { msgs.push("file format specification"); }
@after { msgs.pop(); }
    : KW_SEQUENCEFILE  -> ^(TOK_TBLSEQUENCEFILE)
    | KW_TEXTFILE  -> ^(TOK_TBLTEXTFILE)
    | KW_RCFILE  -> ^(TOK_TBLRCFILE)
    | KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt)
    ;

tabTypeExpr
@init { msgs.push("specifying table types"); }
@after { msgs.pop(); }

   : Identifier (DOT^ (Identifier | KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE))*
   ;

partTypeExpr
@init { msgs.push("specifying table partitions"); }
@after { msgs.pop(); }
    :  tabTypeExpr partitionSpec? -> ^(TOK_TABTYPE tabTypeExpr partitionSpec?)
    ;

descStatement
@init { msgs.push("describe statement"); }
@after { msgs.pop(); }
    : (KW_DESCRIBE|KW_DESC) (isExtended=KW_EXTENDED)? (parttype=partTypeExpr) -> ^(TOK_DESCTABLE $parttype $isExtended?)
    | (KW_DESCRIBE|KW_DESC) KW_FUNCTION KW_EXTENDED? (name=descFuncNames) -> ^(TOK_DESCFUNCTION $name KW_EXTENDED?)
    ;
    
analyzeStatement
@init { msgs.push("analyze statement"); }
@after { msgs.pop(); }
    : KW_ANALYZE KW_TABLE (parttype=partTypeExpr) KW_COMPUTE KW_STATISTICS -> ^(TOK_ANALYZE $parttype)
    ;

showStatement
@init { msgs.push("show statement"); }
@after { msgs.pop(); }
    : KW_SHOW (KW_DATABASES|KW_SCHEMAS) (KW_LIKE showStmtIdentifier)? -> ^(TOK_SHOWDATABASES showStmtIdentifier?)
    | KW_SHOW KW_TABLES showStmtIdentifier?  -> ^(TOK_SHOWTABLES showStmtIdentifier?)
    | KW_SHOW KW_FUNCTIONS showStmtIdentifier?  -> ^(TOK_SHOWFUNCTIONS showStmtIdentifier?)
    | KW_SHOW KW_PARTITIONS Identifier partitionSpec? -> ^(TOK_SHOWPARTITIONS Identifier partitionSpec?)
    | KW_SHOW KW_TABLE KW_EXTENDED ((KW_FROM|KW_IN) db_name=Identifier)? KW_LIKE showStmtIdentifier partitionSpec?
    -> ^(TOK_SHOW_TABLESTATUS showStmtIdentifier $db_name? partitionSpec?)
    | KW_SHOW KW_LOCKS -> ^(TOK_SHOWLOCKS)
    ;

lockStatement
@init { msgs.push("lock statement"); }
@after { msgs.pop(); }
    : KW_LOCK KW_TABLE Identifier partitionSpec? lockMode -> ^(TOK_LOCKTABLE Identifier lockMode partitionSpec?)
    ;

lockMode
@init { msgs.push("lock mode"); }
@after { msgs.pop(); }
    : KW_SHARED | KW_EXCLUSIVE
    ;

unlockStatement
@init { msgs.push("unlock statement"); }
@after { msgs.pop(); }
    : KW_UNLOCK KW_TABLE Identifier partitionSpec?  -> ^(TOK_UNLOCKTABLE Identifier partitionSpec?)
    ;

metastoreCheck
@init { msgs.push("metastore check statement"); }
@after { msgs.pop(); }
    : KW_MSCK (repair=KW_REPAIR)? (KW_TABLE table=Identifier partitionSpec? (COMMA partitionSpec)*)?
    -> ^(TOK_MSCK $repair? ($table partitionSpec*)?)
    ;

createFunctionStatement
@init { msgs.push("create function statement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_TEMPORARY KW_FUNCTION Identifier KW_AS StringLiteral
    -> ^(TOK_CREATEFUNCTION Identifier StringLiteral)
    ;

dropFunctionStatement
@init { msgs.push("drop temporary function statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_TEMPORARY KW_FUNCTION Identifier
    -> ^(TOK_DROPFUNCTION Identifier)
    ;

createViewStatement
@init {
    msgs.push("create view statement");
}
@after { msgs.pop(); }
    : KW_CREATE KW_VIEW ifNotExists? name=Identifier
        (LPAREN columnNameCommentList RPAREN)? tableComment?
        tablePropertiesPrefixed?
        KW_AS
        selectStatement
    -> ^(TOK_CREATEVIEW $name ifNotExists?
         columnNameCommentList?
         tableComment?
         tablePropertiesPrefixed?
         selectStatement
        )
    ;

dropViewStatement
@init { msgs.push("drop view statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_VIEW Identifier
    -> ^(TOK_DROPVIEW Identifier)
    ;

showStmtIdentifier
@init { msgs.push("identifier for show statement"); }
@after { msgs.pop(); }
    : Identifier
    | StringLiteral
    ;

tableComment
@init { msgs.push("table's comment"); }
@after { msgs.pop(); }
    :
      KW_COMMENT comment=StringLiteral  -> ^(TOK_TABLECOMMENT $comment)
    ;

tablePartition
@init { msgs.push("table partition specification"); }
@after { msgs.pop(); }
    : KW_PARTITIONED KW_BY LPAREN columnNameTypeList RPAREN
    -> ^(TOK_TABLEPARTCOLS columnNameTypeList)
    ;

tableBuckets
@init { msgs.push("table buckets specification"); }
@after { msgs.pop(); }
    :
      KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS
    -> ^(TOK_TABLEBUCKETS $bucketCols $sortCols? $num)
    ;

rowFormat
@init { msgs.push("serde specification"); }
@after { msgs.pop(); }
    : rowFormatSerde -> ^(TOK_SERDE rowFormatSerde)
    | rowFormatDelimited -> ^(TOK_SERDE rowFormatDelimited)
    |   -> ^(TOK_SERDE)
    ;

recordReader
@init { msgs.push("record reader specification"); }
@after { msgs.pop(); }
    : KW_RECORDREADER StringLiteral -> ^(TOK_RECORDREADER StringLiteral)
    |   -> ^(TOK_RECORDREADER)
    ;

recordWriter
@init { msgs.push("record writer specification"); }
@after { msgs.pop(); }
    : KW_RECORDWRITER StringLiteral -> ^(TOK_RECORDWRITER StringLiteral)
    |   -> ^(TOK_RECORDWRITER)
    ;

rowFormatSerde
@init { msgs.push("serde format specification"); }
@after { msgs.pop(); }
    : KW_ROW KW_FORMAT KW_SERDE name=StringLiteral (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
    -> ^(TOK_SERDENAME $name $serdeprops?)
    ;

rowFormatDelimited
@init { msgs.push("serde properties specification"); }
@after { msgs.pop(); }
    :
      KW_ROW KW_FORMAT KW_DELIMITED tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier?
    -> ^(TOK_SERDEPROPS tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier?)
    ;

tableRowFormat
@init { msgs.push("table row format specification"); }
@after { msgs.pop(); }
    :
      rowFormatDelimited
    -> ^(TOK_TABLEROWFORMAT rowFormatDelimited)
    | rowFormatSerde
    -> ^(TOK_TABLESERIALIZER rowFormatSerde)
    ;

tablePropertiesPrefixed
@init { msgs.push("table properties with prefix"); }
@after { msgs.pop(); }
    :
        KW_TBLPROPERTIES! tableProperties
    ;

tableProperties
@init { msgs.push("table properties"); }
@after { msgs.pop(); }
    :
      LPAREN propertiesList RPAREN -> ^(TOK_TABLEPROPERTIES propertiesList)
    ;

propertiesList
@init { msgs.push("properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_TABLEPROPLIST keyValueProperty+)
    ;

keyValueProperty
@init { msgs.push("specifying key/value property"); }
@after { msgs.pop(); }
    :
      key=StringLiteral EQUAL value=StringLiteral -> ^(TOK_TABLEPROPERTY $key $value)
    ;

tableRowFormatFieldIdentifier
@init { msgs.push("table row format's field separator"); }
@after { msgs.pop(); }
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=StringLiteral (KW_ESCAPED KW_BY fldEscape=StringLiteral)?
    -> ^(TOK_TABLEROWFORMATFIELD $fldIdnt $fldEscape?)
    ;

tableRowFormatCollItemsIdentifier
@init { msgs.push("table row format's column separator"); }
@after { msgs.pop(); }
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATCOLLITEMS $collIdnt)
    ;

tableRowFormatMapKeysIdentifier
@init { msgs.push("table row format's map key separator"); }
@after { msgs.pop(); }
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATMAPKEYS $mapKeysIdnt)
    ;

tableRowFormatLinesIdentifier
@init { msgs.push("table row format's line separator"); }
@after { msgs.pop(); }
    :
      KW_LINES KW_TERMINATED KW_BY linesIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATLINES $linesIdnt)
    ;

tableFileFormat
@init { msgs.push("table file format specification"); }
@after { msgs.pop(); }
    :
      KW_STORED KW_AS KW_SEQUENCEFILE  -> TOK_TBLSEQUENCEFILE
      | KW_STORED KW_AS KW_TEXTFILE  -> TOK_TBLTEXTFILE
      | KW_STORED KW_AS KW_RCFILE  -> TOK_TBLRCFILE
      | KW_STORED KW_AS KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt)
      | KW_STORED KW_BY storageHandler=StringLiteral
         (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
      -> ^(TOK_STORAGEHANDLER $storageHandler $serdeprops?)
    ;

tableLocation
@init { msgs.push("table location specification"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_TABLELOCATION $locn)
    ;

columnNameTypeList
@init { msgs.push("column name type list"); }
@after { msgs.pop(); }
    : columnNameType (COMMA columnNameType)* -> ^(TOK_TABCOLLIST columnNameType+)
    ;

columnNameColonTypeList
@init { msgs.push("column name type list"); }
@after { msgs.pop(); }
    : columnNameColonType (COMMA columnNameColonType)* -> ^(TOK_TABCOLLIST columnNameColonType+)
    ;

columnNameList
@init { msgs.push("column name list"); }
@after { msgs.pop(); }
    : columnName (COMMA columnName)* -> ^(TOK_TABCOLNAME columnName+)
    ;

columnName
@init { msgs.push("column name"); }
@after { msgs.pop(); }
    :
      Identifier
    ;

columnNameOrderList
@init { msgs.push("column name order list"); }
@after { msgs.pop(); }
    : columnNameOrder (COMMA columnNameOrder)* -> ^(TOK_TABCOLNAME columnNameOrder+)
    ;

columnNameOrder
@init { msgs.push("column name order"); }
@after { msgs.pop(); }
    : Identifier (asc=KW_ASC | desc=KW_DESC)?
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC Identifier)
    ->                  ^(TOK_TABSORTCOLNAMEDESC Identifier)
    ;

columnNameCommentList
@init { msgs.push("column name comment list"); }
@after { msgs.pop(); }
    : columnNameComment (COMMA columnNameComment)* -> ^(TOK_TABCOLNAME columnNameComment+)
    ;

columnNameComment
@init { msgs.push("column name comment"); }
@after { msgs.pop(); }
    : colName=Identifier (KW_COMMENT comment=StringLiteral)?
    -> ^(TOK_TABCOL $colName TOK_NULL $comment?)
    ;

columnRefOrder
@init { msgs.push("column order"); }
@after { msgs.pop(); }
    : expression (asc=KW_ASC | desc=KW_DESC)?
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC expression)
    ->                  ^(TOK_TABSORTCOLNAMEDESC expression)
    ;

columnNameType
@init { msgs.push("column specification"); }
@after { msgs.pop(); }
    : colName=Identifier colType (KW_COMMENT comment=StringLiteral)?
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

columnNameColonType
@init { msgs.push("column specification"); }
@after { msgs.pop(); }
    : colName=Identifier COLON colType (KW_COMMENT comment=StringLiteral)?
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

colType
@init { msgs.push("column type"); }
@after { msgs.pop(); }
    : type
    ;

type
    : primitiveType
    | listType
    | structType
    | mapType;

primitiveType
@init { msgs.push("primitive type specification"); }
@after { msgs.pop(); }
    : KW_TINYINT       ->    TOK_TINYINT
    | KW_SMALLINT      ->    TOK_SMALLINT
    | KW_INT           ->    TOK_INT
    | KW_BIGINT        ->    TOK_BIGINT
    | KW_BOOLEAN       ->    TOK_BOOLEAN
    | KW_FLOAT         ->    TOK_FLOAT
    | KW_DOUBLE        ->    TOK_DOUBLE
    | KW_DATE          ->    TOK_DATE
    | KW_DATETIME      ->    TOK_DATETIME
    | KW_TIMESTAMP     ->    TOK_TIMESTAMP
    | KW_STRING        ->    TOK_STRING
    ;

listType
@init { msgs.push("list type"); }
@after { msgs.pop(); }
    : KW_ARRAY LESSTHAN type GREATERTHAN   -> ^(TOK_LIST type)
    ;

structType
@init { msgs.push("struct type"); }
@after { msgs.pop(); }
    : KW_STRUCT LESSTHAN columnNameColonTypeList GREATERTHAN -> ^(TOK_STRUCT columnNameColonTypeList)
    ;

mapType
@init { msgs.push("map type"); }
@after { msgs.pop(); }
    : KW_MAP LESSTHAN left=primitiveType COMMA right=type GREATERTHAN
    -> ^(TOK_MAP $left $right)
    ;

queryOperator
@init { msgs.push("query operator"); }
@after { msgs.pop(); }
    : KW_UNION KW_ALL -> ^(TOK_UNION)
    ;

// select statement select ... from ... where ... group by ... order by ...
queryStatementExpression
    : queryStatement (queryOperator^ queryStatement)*
    ;

queryStatement
    :
    fromClause
    ( b+=body )+ -> ^(TOK_QUERY fromClause body+)
    | regular_body
    ;

regular_body
   :
   insertClause
   selectClause
   fromClause
   whereClause?
   groupByClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_QUERY fromClause ^(TOK_INSERT insertClause
                     selectClause whereClause? groupByClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?))
   |
   selectStatement
   ;

selectStatement
   :
   selectClause
   fromClause
   whereClause?
   groupByClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_QUERY fromClause ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause whereClause? groupByClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?))
   ;


body
   :
   insertClause
   selectClause
   whereClause?
   groupByClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_INSERT insertClause?
                     selectClause whereClause? groupByClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?)
   |
   selectClause
   whereClause?
   groupByClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause whereClause? groupByClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?)
   ;

insertClause
@init { msgs.push("insert clause"); }
@after { msgs.pop(); }
   :
   KW_INSERT KW_OVERWRITE destination -> ^(TOK_DESTINATION destination)
   ;

destination
@init { msgs.push("destination specification"); }
@after { msgs.pop(); }
   :
     KW_LOCAL KW_DIRECTORY StringLiteral -> ^(TOK_LOCAL_DIR StringLiteral)
   | KW_DIRECTORY StringLiteral -> ^(TOK_DIR StringLiteral)
   | KW_TABLE tabName -> ^(tabName)
   ;

limitClause
@init { msgs.push("limit clause"); }
@after { msgs.pop(); }
   :
   KW_LIMIT num=Number -> ^(TOK_LIMIT $num)
   ;

//----------------------- Rules for parsing selectClause -----------------------------
// select a,b,c ...
selectClause
@init { msgs.push("select clause"); }
@after { msgs.pop(); }
    :
    KW_SELECT hintClause? (((KW_ALL | dist=KW_DISTINCT)? selectList)
                          | (transform=KW_TRANSFORM selectTrfmClause))
     -> {$transform == null && $dist == null}? ^(TOK_SELECT hintClause? selectList)
     -> {$transform == null && $dist != null}? ^(TOK_SELECTDI hintClause? selectList)
     -> ^(TOK_SELECT hintClause? ^(TOK_SELEXPR selectTrfmClause) )
    |
    trfmClause  ->^(TOK_SELECT ^(TOK_SELEXPR trfmClause))
    ;

selectList
@init { msgs.push("select list"); }
@after { msgs.pop(); }
    :
    selectItem ( COMMA  selectItem )* -> selectItem+
    ;

selectTrfmClause
@init { msgs.push("transform clause"); }
@after { msgs.pop(); }
    :
    LPAREN selectExpressionList RPAREN
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde $inRec StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;

hintClause
@init { msgs.push("hint clause"); }
@after { msgs.pop(); }
    :
    DIVIDE STAR PLUS hintList STAR DIVIDE -> ^(TOK_HINTLIST hintList)
    ;

hintList
@init { msgs.push("hint list"); }
@after { msgs.pop(); }
    :
    hintItem (COMMA hintItem)* -> hintItem+
    ;

hintItem
@init { msgs.push("hint item"); }
@after { msgs.pop(); }
    :
    hintName (LPAREN hintArgs RPAREN)? -> ^(TOK_HINT hintName hintArgs?)
    ;

hintName
@init { msgs.push("hint name"); }
@after { msgs.pop(); }
    :
    KW_MAPJOIN -> TOK_MAPJOIN
    | KW_STREAMTABLE -> TOK_STREAMTABLE
    | KW_HOLD_DDLTIME -> TOK_HOLD_DDLTIME
    ;

hintArgs
@init { msgs.push("hint arguments"); }
@after { msgs.pop(); }
    :
    hintArgName (COMMA hintArgName)* -> ^(TOK_HINTARGLIST hintArgName+)
    ;

hintArgName
@init { msgs.push("hint argument name"); }
@after { msgs.pop(); }
    :
    Identifier
    ;

selectItem
@init { msgs.push("selection target"); }
@after { msgs.pop(); }
    :
    ( selectExpression  ((KW_AS? Identifier) | (KW_AS LPAREN Identifier (COMMA Identifier)* RPAREN))?) -> ^(TOK_SELEXPR selectExpression Identifier*)
    ;

trfmClause
@init { msgs.push("transform clause"); }
@after { msgs.pop(); }
    :
    (   KW_MAP    selectExpressionList
      | KW_REDUCE selectExpressionList )
    inSerde=rowFormat inRec=recordWriter
    KW_USING StringLiteral
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))?
    outSerde=rowFormat outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde $inRec StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;

selectExpression
@init { msgs.push("select expression"); }
@after { msgs.pop(); }
    :
    expression | tableAllColumns
    ;

selectExpressionList
@init { msgs.push("select expression list"); }
@after { msgs.pop(); }
    :
    selectExpression (COMMA selectExpression)* -> ^(TOK_EXPLIST selectExpression+)
    ;


//-----------------------------------------------------------------------------------

tableAllColumns
    :
    STAR -> ^(TOK_ALLCOLREF)
    | Identifier DOT STAR -> ^(TOK_ALLCOLREF Identifier)
    ;

// (table|column)
tableOrColumn
@init { msgs.push("table or column identifier"); }
@after { msgs.pop(); }
    :
    Identifier -> ^(TOK_TABLE_OR_COL Identifier)
    ;

expressionList
@init { msgs.push("expression list"); }
@after { msgs.pop(); }
    :
    expression (COMMA expression)* -> ^(TOK_EXPLIST expression+)
    ;

aliasList
@init { msgs.push("alias list"); }
@after { msgs.pop(); }
    :
    Identifier (COMMA Identifier)* -> ^(TOK_ALIASLIST Identifier+)
    ;

//----------------------- Rules for parsing fromClause ------------------------------
// from [col1, col2, col3] table1, [col4, col5] table2
fromClause
@init { msgs.push("from clause"); }
@after { msgs.pop(); }
    :
    KW_FROM joinSource -> ^(TOK_FROM joinSource)
    ;

joinSource
@init { msgs.push("join source"); }
@after { msgs.pop(); }
    : fromSource ( joinToken^ fromSource (KW_ON! expression)? )*
    | uniqueJoinToken^ uniqueJoinSource (COMMA! uniqueJoinSource)+
    ;

uniqueJoinSource
@init { msgs.push("join source"); }
@after { msgs.pop(); }
    : KW_PRESERVE? fromSource uniqueJoinExpr
    ;

uniqueJoinExpr
@init { msgs.push("unique join expression list"); }
@after { msgs.pop(); }
    : LPAREN e1+=expression (COMMA e1+=expression)* RPAREN
      -> ^(TOK_EXPLIST $e1*)
    ;

uniqueJoinToken
@init { msgs.push("unique join"); }
@after { msgs.pop(); }
    : KW_UNIQUEJOIN -> TOK_UNIQUEJOIN;

joinToken
@init { msgs.push("join type specifier"); }
@after { msgs.pop(); }
    :
      KW_JOIN                     -> TOK_JOIN
    | KW_LEFT  KW_OUTER KW_JOIN   -> TOK_LEFTOUTERJOIN
    | KW_RIGHT KW_OUTER KW_JOIN   -> TOK_RIGHTOUTERJOIN
    | KW_FULL  KW_OUTER KW_JOIN   -> TOK_FULLOUTERJOIN
    | KW_LEFT  KW_SEMI  KW_JOIN   -> TOK_LEFTSEMIJOIN
    ;

lateralView
@init {msgs.push("lateral view"); }
@after {msgs.pop(); }
	:
	KW_LATERAL KW_VIEW function tableAlias KW_AS Identifier (COMMA Identifier)* -> ^(TOK_LATERAL_VIEW ^(TOK_SELECT ^(TOK_SELEXPR function Identifier+ tableAlias)))
	;

tableAlias
@init {msgs.push("table alias"); }
@after {msgs.pop(); }
    :
    Identifier -> ^(TOK_TABALIAS Identifier)
    ;

fromSource
@init { msgs.push("from source"); }
@after { msgs.pop(); }
    :
    (tableSource | subQuerySource) (lateralView^)*
    ;

tableSample
@init { msgs.push("table sample specification"); }
@after { msgs.pop(); }
    :
    KW_TABLESAMPLE LPAREN KW_BUCKET (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_ON expr+=expression (COMMA expr+=expression)*)? RPAREN -> ^(TOK_TABLESAMPLE $numerator $denominator $expr*)
    ;

tableSource
@init { msgs.push("table source"); }
@after { msgs.pop(); }
    :
    tabname=Identifier (ts=tableSample)? (alias=Identifier)? -> ^(TOK_TABREF $tabname $ts? $alias?)

    ;

subQuerySource
@init { msgs.push("subquery source"); }
@after { msgs.pop(); }
    :
    LPAREN queryStatementExpression RPAREN Identifier -> ^(TOK_SUBQUERY queryStatementExpression Identifier)
    ;

//----------------------- Rules for parsing whereClause -----------------------------
// where a=b and ...
whereClause
@init { msgs.push("where clause"); }
@after { msgs.pop(); }
    :
    KW_WHERE searchCondition -> ^(TOK_WHERE searchCondition)
    ;

searchCondition
@init { msgs.push("search condition"); }
@after { msgs.pop(); }
    :
    expression
    ;

//-----------------------------------------------------------------------------------

// group by a,b
groupByClause
@init { msgs.push("group by clause"); }
@after { msgs.pop(); }
    :
    KW_GROUP KW_BY
    groupByExpression
    ( COMMA groupByExpression )*
    -> ^(TOK_GROUPBY groupByExpression+)
    ;

groupByExpression
@init { msgs.push("group by expression"); }
@after { msgs.pop(); }
    :
    expression
    ;

// order by a,b
orderByClause
@init { msgs.push("order by clause"); }
@after { msgs.pop(); }
    :
    KW_ORDER KW_BY
    columnRefOrder
    ( COMMA columnRefOrder)* -> ^(TOK_ORDERBY columnRefOrder+)
    ;

clusterByClause
@init { msgs.push("cluster by clause"); }
@after { msgs.pop(); }
    :
    KW_CLUSTER KW_BY
    expression
    ( COMMA expression )* -> ^(TOK_CLUSTERBY expression+)
    ;

distributeByClause
@init { msgs.push("distribute by clause"); }
@after { msgs.pop(); }
    :
    KW_DISTRIBUTE KW_BY
    expression (COMMA expression)* -> ^(TOK_DISTRIBUTEBY expression+)
    ;

sortByClause
@init { msgs.push("sort by clause"); }
@after { msgs.pop(); }
    :
    KW_SORT KW_BY
    columnRefOrder
    ( COMMA columnRefOrder)* -> ^(TOK_SORTBY columnRefOrder+)
    ;

// fun(par1, par2, par3)
function
@init { msgs.push("function specification"); }
@after { msgs.pop(); }
    :
    functionName
    LPAREN
      (
        (star=STAR)
        | (dist=KW_DISTINCT)? (expression (COMMA expression)*)?
      )
    RPAREN -> {$star != null}? ^(TOK_FUNCTIONSTAR functionName)
           -> {$dist == null}? ^(TOK_FUNCTION functionName (expression+)?)
                            -> ^(TOK_FUNCTIONDI functionName (expression+)?)
    ;

functionName
@init { msgs.push("function name"); }
@after { msgs.pop(); }
    : // Keyword IF is also a function name
    Identifier | KW_IF | KW_ARRAY | KW_MAP | KW_STRUCT
    ;

castExpression
@init { msgs.push("cast expression"); }
@after { msgs.pop(); }
    :
    KW_CAST
    LPAREN
          expression
          KW_AS
          primitiveType
    RPAREN -> ^(TOK_FUNCTION primitiveType expression)
    ;

caseExpression
@init { msgs.push("case expression"); }
@after { msgs.pop(); }
    :
    KW_CASE expression
    (KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_CASE expression*)
    ;

whenExpression
@init { msgs.push("case expression"); }
@after { msgs.pop(); }
    :
    KW_CASE
     ( KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_WHEN expression*)
    ;

constant
@init { msgs.push("constant"); }
@after { msgs.pop(); }
    :
    Number
    | StringLiteral
    | charSetStringLiteral
    | booleanValue
    ;

charSetStringLiteral
@init { msgs.push("character string literal"); }
@after { msgs.pop(); }
    :
    csName=CharSetName csLiteral=CharSetLiteral -> ^(TOK_CHARSETLITERAL $csName $csLiteral)
    ;

expression
@init { msgs.push("expression specification"); }
@after { msgs.pop(); }
    :
    precedenceOrExpression
    ;

atomExpression
    :
    KW_NULL -> TOK_NULL
    | constant
    | function
    | castExpression
    | caseExpression
    | whenExpression
    | tableOrColumn
    | LPAREN! expression RPAREN!
    ;


precedenceFieldExpression
    :
    atomExpression ((LSQUARE^ expression RSQUARE!) | (DOT^ Identifier))*
    ;

precedenceUnaryOperator
    :
    PLUS | MINUS | TILDE
    ;

nullCondition
    :
    KW_NULL -> ^(TOK_ISNULL)
    | KW_NOT KW_NULL -> ^(TOK_ISNOTNULL)
    ;

precedenceUnaryPrefixExpression
    :
    (precedenceUnaryOperator^)* precedenceFieldExpression
    ;

precedenceUnarySuffixExpression
    : precedenceUnaryPrefixExpression (a=KW_IS nullCondition)?
    -> {$a != null}? ^(TOK_FUNCTION nullCondition precedenceUnaryPrefixExpression)
    -> precedenceUnaryPrefixExpression
    ;


precedenceBitwiseXorOperator
    :
    BITWISEXOR
    ;

precedenceBitwiseXorExpression
    :
    precedenceUnarySuffixExpression (precedenceBitwiseXorOperator^ precedenceUnarySuffixExpression)*
    ;


precedenceStarOperator
    :
    STAR | DIVIDE | MOD | DIV
    ;

precedenceStarExpression
    :
    precedenceBitwiseXorExpression (precedenceStarOperator^ precedenceBitwiseXorExpression)*
    ;


precedencePlusOperator
    :
    PLUS | MINUS
    ;

precedencePlusExpression
    :
    precedenceStarExpression (precedencePlusOperator^ precedenceStarExpression)*
    ;


precedenceAmpersandOperator
    :
    AMPERSAND
    ;

precedenceAmpersandExpression
    :
    precedencePlusExpression (precedenceAmpersandOperator^ precedencePlusExpression)*
    ;


precedenceBitwiseOrOperator
    :
    BITWISEOR
    ;

precedenceBitwiseOrExpression
    :
    precedenceAmpersandExpression (precedenceBitwiseOrOperator^ precedenceAmpersandExpression)*
    ;


precedenceEqualOperator
    :
    EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    | KW_LIKE | KW_RLIKE | KW_REGEXP
    ;

precedenceEqualExpression
    :
    precedenceBitwiseOrExpression ( (precedenceEqualOperator^ precedenceBitwiseOrExpression) | (inOperator^ expressions) )*
    ;

inOperator
    :
    KW_IN -> ^(TOK_FUNCTION KW_IN)
    ;

expressions
    :
    LPAREN expression (COMMA expression)* RPAREN -> expression*
    ;

precedenceNotOperator
    :
    KW_NOT
    ;

precedenceNotExpression
    :
    (precedenceNotOperator^)* precedenceEqualExpression
    ;


precedenceAndOperator
    :
    KW_AND
    ;

precedenceAndExpression
    :
    precedenceNotExpression (precedenceAndOperator^ precedenceNotExpression)*
    ;


precedenceOrOperator
    :
    KW_OR
    ;

precedenceOrExpression
    :
    precedenceAndExpression (precedenceOrOperator^ precedenceAndExpression)*
    ;


booleanValue
    :
    KW_TRUE^ | KW_FALSE^
    ;

tabName
   :
   Identifier partitionSpec? -> ^(TOK_TAB Identifier partitionSpec?)
   ;

partitionSpec
    :
    KW_PARTITION
     LPAREN partitionVal (COMMA  partitionVal )* RPAREN -> ^(TOK_PARTSPEC partitionVal +)
    ;

partitionVal
    :
    Identifier (EQUAL constant)? -> ^(TOK_PARTVAL Identifier constant?)
    ;

sysFuncNames
    :
      KW_AND
    | KW_OR
    | KW_NOT
    | KW_LIKE
    | KW_IF
    | KW_CASE
    | KW_WHEN
    | KW_TINYINT
    | KW_SMALLINT
    | KW_INT
    | KW_BIGINT
    | KW_FLOAT
    | KW_DOUBLE
    | KW_BOOLEAN
    | KW_STRING
    | KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    | EQUAL
    | NOTEQUAL
    | LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    | DIVIDE
    | PLUS
    | MINUS
    | STAR
    | MOD
    | DIV
    | AMPERSAND
    | TILDE
    | BITWISEOR
    | BITWISEXOR
    | KW_RLIKE
    | KW_REGEXP
    | KW_IN
    ;

descFuncNames
    :
      sysFuncNames
    | StringLiteral
    | Identifier
    ;

// Keywords
KW_TRUE : 'TRUE';
KW_FALSE : 'FALSE';
KW_ALL : 'ALL';
KW_AND : 'AND';
KW_OR : 'OR';
KW_NOT : 'NOT' | '!';
KW_LIKE : 'LIKE';

KW_IF : 'IF';
KW_EXISTS : 'EXISTS';

KW_ASC : 'ASC';
KW_DESC : 'DESC';
KW_ORDER : 'ORDER';
KW_BY : 'BY';
KW_GROUP : 'GROUP';
KW_WHERE : 'WHERE';
KW_FROM : 'FROM';
KW_AS : 'AS';
KW_SELECT : 'SELECT';
KW_DISTINCT : 'DISTINCT';
KW_INSERT : 'INSERT';
KW_OVERWRITE : 'OVERWRITE';
KW_OUTER : 'OUTER';
KW_UNIQUEJOIN : 'UNIQUEJOIN';
KW_PRESERVE : 'PRESERVE';
KW_JOIN : 'JOIN';
KW_LEFT : 'LEFT';
KW_RIGHT : 'RIGHT';
KW_FULL : 'FULL';
KW_ON : 'ON';
KW_PARTITION : 'PARTITION';
KW_PARTITIONS : 'PARTITIONS';
KW_TABLE: 'TABLE';
KW_TABLES: 'TABLES';
KW_INDEX: 'INDEX';
KW_REBUILD: 'REBUILD';
KW_FUNCTIONS: 'FUNCTIONS';
KW_SHOW: 'SHOW';
KW_MSCK: 'MSCK';
KW_REPAIR: 'REPAIR';
KW_DIRECTORY: 'DIRECTORY';
KW_LOCAL: 'LOCAL';
KW_TRANSFORM : 'TRANSFORM';
KW_USING: 'USING';
KW_CLUSTER: 'CLUSTER';
KW_DISTRIBUTE: 'DISTRIBUTE';
KW_SORT: 'SORT';
KW_UNION: 'UNION';
KW_LOAD: 'LOAD';
KW_DATA: 'DATA';
KW_INPATH: 'INPATH';
KW_IS: 'IS';
KW_NULL: 'NULL';
KW_CREATE: 'CREATE';
KW_EXTERNAL: 'EXTERNAL';
KW_ALTER: 'ALTER';
KW_CHANGE: 'CHANGE';
KW_COLUMN: 'COLUMN';
KW_FIRST: 'FIRST';
KW_AFTER: 'AFTER';
KW_DESCRIBE: 'DESCRIBE';
KW_DROP: 'DROP';
KW_RENAME: 'RENAME';
KW_TO: 'TO';
KW_COMMENT: 'COMMENT';
KW_BOOLEAN: 'BOOLEAN';
KW_TINYINT: 'TINYINT';
KW_SMALLINT: 'SMALLINT';
KW_INT: 'INT';
KW_BIGINT: 'BIGINT';
KW_FLOAT: 'FLOAT';
KW_DOUBLE: 'DOUBLE';
KW_DATE: 'DATE';
KW_DATETIME: 'DATETIME';
KW_TIMESTAMP: 'TIMESTAMP';
KW_STRING: 'STRING';
KW_ARRAY: 'ARRAY';
KW_STRUCT: 'STRUCT';
KW_MAP: 'MAP';
KW_REDUCE: 'REDUCE';
KW_PARTITIONED: 'PARTITIONED';
KW_CLUSTERED: 'CLUSTERED';
KW_SORTED: 'SORTED';
KW_INTO: 'INTO';
KW_BUCKETS: 'BUCKETS';
KW_ROW: 'ROW';
KW_FORMAT: 'FORMAT';
KW_DELIMITED: 'DELIMITED';
KW_FIELDS: 'FIELDS';
KW_TERMINATED: 'TERMINATED';
KW_ESCAPED: 'ESCAPED';
KW_COLLECTION: 'COLLECTION';
KW_ITEMS: 'ITEMS';
KW_KEYS: 'KEYS';
KW_KEY_TYPE: '$KEY$';
KW_LINES: 'LINES';
KW_STORED: 'STORED';
KW_FILEFORMAT: 'FILEFORMAT';
KW_SEQUENCEFILE: 'SEQUENCEFILE';
KW_TEXTFILE: 'TEXTFILE';
KW_RCFILE: 'RCFILE';
KW_INPUTFORMAT: 'INPUTFORMAT';
KW_OUTPUTFORMAT: 'OUTPUTFORMAT';
KW_OFFLINE: 'OFFLINE';
KW_ENABLE: 'ENABLE';
KW_DISABLE: 'DISABLE';
KW_READONLY: 'READONLY';
KW_NO_DROP: 'NO_DROP';
KW_LOCATION: 'LOCATION';
KW_TABLESAMPLE: 'TABLESAMPLE';
KW_BUCKET: 'BUCKET';
KW_OUT: 'OUT';
KW_OF: 'OF';
KW_CAST: 'CAST';
KW_ADD: 'ADD';
KW_REPLACE: 'REPLACE';
KW_COLUMNS: 'COLUMNS';
KW_RLIKE: 'RLIKE';
KW_REGEXP: 'REGEXP';
KW_TEMPORARY: 'TEMPORARY';
KW_FUNCTION: 'FUNCTION';
KW_EXPLAIN: 'EXPLAIN';
KW_EXTENDED: 'EXTENDED';
KW_SERDE: 'SERDE';
KW_WITH: 'WITH';
KW_DEFERRED: 'DEFERRED';
KW_SERDEPROPERTIES: 'SERDEPROPERTIES';
KW_LIMIT: 'LIMIT';
KW_SET: 'SET';
KW_TBLPROPERTIES: 'TBLPROPERTIES';
KW_IDXPROPERTIES: 'INDEXPROPERTIES';
KW_VALUE_TYPE: '$VALUE$';
KW_ELEM_TYPE: '$ELEM$';
KW_CASE: 'CASE';
KW_WHEN: 'WHEN';
KW_THEN: 'THEN';
KW_ELSE: 'ELSE';
KW_END: 'END';
KW_MAPJOIN: 'MAPJOIN';
KW_STREAMTABLE: 'STREAMTABLE';
KW_HOLD_DDLTIME: 'HOLD_DDLTIME';
KW_CLUSTERSTATUS: 'CLUSTERSTATUS';
KW_UTC: 'UTC';
KW_UTCTIMESTAMP: 'UTC_TMESTAMP';
KW_LONG: 'LONG';
KW_DELETE: 'DELETE';
KW_PLUS: 'PLUS';
KW_MINUS: 'MINUS';
KW_FETCH: 'FETCH';
KW_INTERSECT: 'INTERSECT';
KW_VIEW: 'VIEW';
KW_IN: 'IN';
KW_DATABASE: 'DATABASE';
KW_DATABASES: 'DATABASES';
KW_MATERIALIZED: 'MATERIALIZED';
KW_SCHEMA: 'SCHEMA';
KW_SCHEMAS: 'SCHEMAS';
KW_GRANT: 'GRANT';
KW_REVOKE: 'REVOKE';
KW_SSL: 'SSL';
KW_UNDO: 'UNDO';
KW_LOCK: 'LOCK';
KW_LOCKS: 'LOCKS';
KW_UNLOCK: 'UNLOCK';
KW_SHARED: 'SHARED';
KW_EXCLUSIVE: 'EXCLUSIVE';
KW_PROCEDURE: 'PROCEDURE';
KW_UNSIGNED: 'UNSIGNED';
KW_WHILE: 'WHILE';
KW_READ: 'READ';
KW_READS: 'READS';
KW_PURGE: 'PURGE';
KW_RANGE: 'RANGE';
KW_ANALYZE: 'ANALYZE';
KW_BEFORE: 'BEFORE';
KW_BETWEEN: 'BETWEEN';
KW_BOTH: 'BOTH';
KW_BINARY: 'BINARY';
KW_CROSS: 'CROSS';
KW_CONTINUE: 'CONTINUE';
KW_CURSOR: 'CURSOR';
KW_TRIGGER: 'TRIGGER';
KW_RECORDREADER: 'RECORDREADER';
KW_RECORDWRITER: 'RECORDWRITER';
KW_SEMI: 'SEMI';
KW_LATERAL: 'LATERAL';
KW_TOUCH: 'TOUCH';
KW_ARCHIVE: 'ARCHIVE';
KW_UNARCHIVE: 'UNARCHIVE';
KW_COMPUTE: 'COMPUTE';
KW_STATISTICS: 'STATISTICS';
KW_USE: 'USE';

// Operators
// NOTE: if you add a new function/operator, add it to sysFuncNames so that describe function _FUNC_ will work.

DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';

EQUAL : '=' | '==';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

DIVIDE : '/';
PLUS : '+';
MINUS : '-';
STAR : '*';
MOD : '%';
DIV : 'DIV';

AMPERSAND : '&';
TILDE : '~';
BITWISEOR : '|';
BITWISEXOR : '^';
QUESTION : '?';
DOLLAR : '$';

// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
HexDigit
    : 'a'..'f' | 'A'..'F'
    ;

fragment
Digit
    :
    '0'..'9'
    ;

fragment
Exponent
    :
    'e' ( PLUS|MINUS )? (Digit)+
    ;

fragment
RegexComponent
    : 'a'..'z' | 'A'..'Z' | '0'..'9' | '_'
    | PLUS | STAR | QUESTION | MINUS | DOT
    | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY
    | BITWISEXOR | BITWISEOR | DOLLAR
    ;

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    )+
    ;

CharSetLiteral
    :
    StringLiteral
    | '0' 'X' (HexDigit|Digit)+
    ;

Number
    :
    (Digit)+ ( DOT (Digit)* (Exponent)? | Exponent)?
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_')*
    | '`' RegexComponent+ '`'
    ;

CharSetName
    :
    '_' (Letter | Digit | '_' | '-' | '.' | ':' )+
    ;

WS  :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}
    ;

COMMENT
  : '--' (~('\n'|'\r'))*
    { $channel=HIDDEN; }
  ;


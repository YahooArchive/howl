package org.apache.hadoop.hive.howl.common;

import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class TestHowlUtil extends TestCase{


  public void testFsPermissionOperation(){

    HashMap<String,Integer> permsCode = new HashMap<String,Integer>();

    for (int i = 0; i < 8; i++){
      for (int j = 0; j < 8; j++){
        for (int k = 0; k < 8; k++){
          StringBuilder sb = new StringBuilder();
          sb.append("0");
          sb.append(i);
          sb.append(j);
          sb.append(k);
          Integer code = (((i*8)+j)*8)+k;
          String perms = (new FsPermission(Short.decode(sb.toString()))).toString();
          if (permsCode.containsKey(perms)){
            assertEquals("permissions(" + perms + ") mapped to multiple codes",code,permsCode.get(perms));
          }
          permsCode.put(perms, code);
          assertFsPermissionTransformationIsGood(perms);
        }
      }
    }
  }

  private void assertFsPermissionTransformationIsGood(String perms) {
    assertEquals(perms,FsPermission.valueOf("-"+perms).toString());
  }

  public void testValidateMorePermissive(){
    assertConsistentFsPermissionBehaviour(FsAction.ALL,true,true,true,true,true,true,true,true);
    assertConsistentFsPermissionBehaviour(FsAction.READ,false,true,false,true,false,false,false,false);
    assertConsistentFsPermissionBehaviour(FsAction.WRITE,false,true,false,false,true,false,false,false);
    assertConsistentFsPermissionBehaviour(FsAction.EXECUTE,false,true,true,false,false,false,false,false);
    assertConsistentFsPermissionBehaviour(FsAction.READ_EXECUTE,false,true,true,true,false,true,false,false);
    assertConsistentFsPermissionBehaviour(FsAction.READ_WRITE,false,true,false,true,true,false,true,false);
    assertConsistentFsPermissionBehaviour(FsAction.WRITE_EXECUTE,false,true,true,false,true,false,false,true);
    assertConsistentFsPermissionBehaviour(FsAction.NONE,false,true,false,false,false,false,false,false);
  }


  private void assertConsistentFsPermissionBehaviour(
      FsAction base, boolean versusAll, boolean versusNone,
      boolean versusX, boolean versusR, boolean versusW,
      boolean versusRX, boolean versusRW,  boolean versusWX){

    assertTrue(versusAll == HowlUtil.validateMorePermissive(base, FsAction.ALL));
    assertTrue(versusX == HowlUtil.validateMorePermissive(base, FsAction.EXECUTE));
    assertTrue(versusNone == HowlUtil.validateMorePermissive(base, FsAction.NONE));
    assertTrue(versusR == HowlUtil.validateMorePermissive(base, FsAction.READ));
    assertTrue(versusRX == HowlUtil.validateMorePermissive(base, FsAction.READ_EXECUTE));
    assertTrue(versusRW == HowlUtil.validateMorePermissive(base, FsAction.READ_WRITE));
    assertTrue(versusW == HowlUtil.validateMorePermissive(base, FsAction.WRITE));
    assertTrue(versusWX == HowlUtil.validateMorePermissive(base, FsAction.WRITE_EXECUTE));
  }


}

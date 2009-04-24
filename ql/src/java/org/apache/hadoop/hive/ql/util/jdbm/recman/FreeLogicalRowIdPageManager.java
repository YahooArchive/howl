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

/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot. 
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id: FreeLogicalRowIdPageManager.java,v 1.1 2000/05/06 00:00:31 boisvert Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import java.io.IOException;

/**
 *  This class manages free Logical rowid pages and provides methods
 *  to free and allocate Logical rowids on a high level.
 */
final class FreeLogicalRowIdPageManager {
    // our record file
    private RecordFile file;
    // our page manager
    private PageManager pageman;

    /**
     *  Creates a new instance using the indicated record file and
     *  page manager.
     */
    FreeLogicalRowIdPageManager(RecordFile file,
                                PageManager pageman) throws IOException {
        this.file = file;
        this.pageman = pageman;
    }

    /**
     *  Returns a free Logical rowid, or
     *  null if nothing was found.
     */
    Location get() throws IOException {
  
        // Loop through the free Logical rowid list until we find
        // the first rowid.
        Location retval = null;
        PageCursor curs = new PageCursor(pageman, Magic.FREELOGIDS_PAGE);
        while (curs.next() != 0) {
            FreeLogicalRowIdPage fp = FreeLogicalRowIdPage
                .getFreeLogicalRowIdPageView(file.get(curs.getCurrent()));
            int slot = fp.getFirstAllocated();
            if (slot != -1) {
                // got one!
                retval =
                    new Location(fp.get(slot));
                fp.free(slot);
                if (fp.getCount() == 0) {
                    // page became empty - free it
                    file.release(curs.getCurrent(), false);
                    pageman.free(Magic.FREELOGIDS_PAGE, curs.getCurrent());
                }
                else
                    file.release(curs.getCurrent(), true);
                
                return retval;
            }
            else {
                // no luck, go to next page
                file.release(curs.getCurrent(), false);
            }     
        }
        return null;
    }

    /**
     *  Puts the indicated rowid on the free list
     */
    void put(Location rowid)
    throws IOException {
        
        PhysicalRowId free = null;
        PageCursor curs = new PageCursor(pageman, Magic.FREELOGIDS_PAGE);
        long freePage = 0;
        while (curs.next() != 0) {
            freePage = curs.getCurrent();
            BlockIo curBlock = file.get(freePage);
            FreeLogicalRowIdPage fp = FreeLogicalRowIdPage
                .getFreeLogicalRowIdPageView(curBlock);
            int slot = fp.getFirstFree();
            if (slot != -1) {
                free = fp.alloc(slot);
                break;
            }
            
            file.release(curBlock);
        }
        if (free == null) {
            // No more space on the free list, add a page.
            freePage = pageman.allocate(Magic.FREELOGIDS_PAGE);
            BlockIo curBlock = file.get(freePage);
            FreeLogicalRowIdPage fp = 
                FreeLogicalRowIdPage.getFreeLogicalRowIdPageView(curBlock);
            free = fp.alloc(0);
        }
        free.setBlock(rowid.getBlock());
        free.setOffset(rowid.getOffset());
        file.release(freePage, true);
    }
}

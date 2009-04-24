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
 * $Id: PhysicalRowId.java,v 1.1 2000/05/06 00:00:31 boisvert Exp $
 */

package org.apache.hadoop.hive.ql.util.jdbm.recman;

/**
 *  A physical rowid is nothing else than a pointer to a physical location
 *  in a file - a (block, offset) tuple.
 *  <P>
 *  <B>Note</B>: The fact that the offset is modelled as a short limits 
 *  the block size to 32k.
 */
class PhysicalRowId {
    // offsets
    private static final short O_BLOCK = 0; // long block
    private static final short O_OFFSET = Magic.SZ_LONG; // short offset
    static final int SIZE = O_OFFSET + Magic.SZ_SHORT;
    
    // my block and the position within the block
    BlockIo block;
    short pos;

    /**
     *  Constructs a physical rowid from the indicated data starting at
     *  the indicated position.
     */
    PhysicalRowId(BlockIo block, short pos) {
        this.block = block;
        this.pos = pos;
    }
    
    /** Returns the block number */
    long getBlock() {
        return block.readLong(pos + O_BLOCK);
    }
    
    /** Sets the block number */
    void setBlock(long value) {
        block.writeLong(pos + O_BLOCK, value);
    }
    
    /** Returns the offset */
    short getOffset() {
        return block.readShort(pos + O_OFFSET);
    }
    
    /** Sets the offset */
    void setOffset(short value) {
        block.writeShort(pos + O_OFFSET, value);
    }
}

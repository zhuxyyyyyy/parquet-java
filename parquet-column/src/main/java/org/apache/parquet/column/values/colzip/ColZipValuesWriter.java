/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.colzip;

import java.nio.charset.Charset;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ColZip encoding only for ByteArray
 */
public class ColZipValuesWriter extends ValuesWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ColZipValuesWriter.class);

  public static final Charset CHARSET = Charset.forName("UTF-8");

  private static final ColZipCppWrapper cppWrapper = new ColZipCppWrapper();

  private CapacityByteArrayOutputStream arrayOut;

  private double memThreshold = 0.5;
  private int srcBufCapacity = 16 * 1024 * 1024;
  private byte[] srcBuf = new byte[srcBufCapacity];
  private int srcBufSize = 0;

  public ColZipValuesWriter(int initialSize, int pageSize, ByteBufferAllocator allocator) {
    //     memThreshold = 1.0 * pageSize / srcBufCapacity;
    arrayOut = new CapacityByteArrayOutputStream(initialSize, pageSize, allocator);
  }

  @Override
  public final void writeBytes(Binary v) {
    byte[] vBytes = v.getBytes();
    System.arraycopy(vBytes, 0, srcBuf, srcBufSize, vBytes.length);
    srcBufSize += vBytes.length;
    srcBuf[srcBufSize++] = '\n';
  }

  @Override
  public long getBufferedSize() {
    if (srcBufSize > srcBufCapacity * memThreshold) {
      return srcBufSize;
    } else {
      return 1;
    }
  }

  @Override
  public BytesInput getBytes() {
    // add ColZip code here
    byte[] dstBuf = cppWrapper.NativeColZipEncode(srcBuf, srcBufSize);
    arrayOut.write(dstBuf, 0, dstBuf.length);
    if (LOG.isDebugEnabled()) LOG.debug("writing a buffer of size {}", arrayOut.size());
    return BytesInput.from(arrayOut);
  }

  @Override
  public void reset() {
    arrayOut.reset();
  }

  @Override
  public void close() {
    arrayOut.close();
  }

  @Override
  public long getAllocatedSize() {
    return arrayOut.getCapacity();
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.COLZIP;
  }

  @Override
  public String memUsageString(String prefix) {
    return arrayOut.memUsageString(prefix + " COLZIP");
  }
}

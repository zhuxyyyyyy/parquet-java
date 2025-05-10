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

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColZipValuesReader extends ValuesReader {
  private static final Logger LOG = LoggerFactory.getLogger(ColZipValuesReader.class);
  private static final ColZipCppWrapper cppWrapper = new ColZipCppWrapper();
  private long readerInstance;

  private byte[] getNextElement() {
    return cppWrapper.ColZipDecodeGetNext(readerInstance);
  }

  @Override
  public Binary readBytes() {
    byte[] bytes = getNextElement();
    return Binary.fromConstantByteArray(bytes);
  }

  @Override
  public void skip() {
    getNextElement();
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug(
        "init from page at offset {} for length {}",
        stream.position(),
        (stream.available() - stream.position()));
    int pageSize = (int) (stream.available());
    byte[] bytes = new byte[pageSize];
    int actualPageSize = stream.read(bytes, 0, pageSize);
    LOG.debug("bytes len: {}, actual bytes len: {}", bytes.length, actualPageSize);
    readerInstance = cppWrapper.InitColZipDecoder(bytes, bytes.length);
  }
}

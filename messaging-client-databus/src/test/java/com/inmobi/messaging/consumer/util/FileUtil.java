package com.inmobi.messaging.consumer.util;

/*
 * #%L
 * messaging-client-databus
 * %%
 * Copyright (C) 2012 - 2014 InMobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class FileUtil {
  private static final Log LOG = LogFactory.getLog(FileUtil.class);

  public static void gzip(Path src, Path target, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream out = fs.create(target);
    GzipCodec gzipCodec = (GzipCodec) ReflectionUtils.newInstance(
        GzipCodec.class, conf);
    Compressor gzipCompressor = CodecPool.getCompressor(gzipCodec);
    OutputStream compressedOut = gzipCodec.createOutputStream(out,
        gzipCompressor);
    FSDataInputStream in = fs.open(src);
    try {
      IOUtils.copyBytes(in, compressedOut, conf);
    } catch (Exception e) {
      LOG.error("Error in compressing ", e);
    } finally {
      in.close();
      CodecPool.returnCompressor(gzipCompressor);
      compressedOut.close();
      out.close();
    }
  }
}


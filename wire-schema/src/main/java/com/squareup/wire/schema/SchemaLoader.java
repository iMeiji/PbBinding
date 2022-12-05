/*
 * Copyright (C) 2015 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.squareup.wire.schema;

import com.google.common.io.Closer;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import okio.BufferedSource;
import okio.Okio;
import okio.Source;

import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Load proto files and their transitive dependencies, parse them, and link them together.
 *
 * <p>To find proto files to load, a non-empty set of sources are searched. Each source is
 * either a regular directory or a ZIP file. Within ZIP files, proto files are expected to be found
 * relative to the root of the archive.
 */
public final class SchemaLoader {
  static final String DESCRIPTOR_PROTO = "google/protobuf/descriptor.proto";
  static final String EMPTY_PROTO = "google/protobuf/empty.proto";
  static final String FIELD_MASK_PROTO = "google/protobuf/field_mask.proto";

  private final List<Path> sources = new ArrayList<>();
  private final List<String> protos = new ArrayList<>();

  /** Add directory or zip file source from which proto files will be loaded. */
  public SchemaLoader addSource(File file) {
    return addSource(file.toPath());
  }

  /** Add directory or zip file source from which proto files will be loaded. */
  public SchemaLoader addSource(Path path) {
    sources.add(path);
    return this;
  }

  /** Returns a mutable list of the sources that this loader will load from. */
  public List<Path> sources() {
    return sources;
  }

  /**
   * Add a proto file to load. Dependencies will be loaded automatically from the configured
   * sources.
   */
  public SchemaLoader addProto(String proto) {
    protos.add(proto);
    return this;
  }

  /** Returns a mutable list of the protos that this loader will load. */
  public List<String> protos() {
    return protos;
  }

  public Schema load() throws IOException {
    if (sources.isEmpty()) {
      throw new IllegalStateException("No sources added.");
    }

    try (Closer closer = Closer.create()) {
      // Map the physical path to the file system root. For regular directories the key and the
      // value are equal. For ZIP files the key is the path to the .zip, and the value is the root
      // of the file system within it.
      Map<Path, Path> directories = new LinkedHashMap<>();
      for (Path source : sources) {
        if (Files.isRegularFile(source)) {
          FileSystem sourceFs = FileSystems.newFileSystem(source, getClass().getClassLoader());
          closer.register(sourceFs);
          directories.put(source, getOnlyElement(sourceFs.getRootDirectories()));
        } else {
          directories.put(source, source);
        }
      }
      return loadFromDirectories(directories);
    }
  }

  private Schema loadFromDirectories(Map<Path, Path> directories) throws IOException {
    final Deque<String> protos = new ArrayDeque<>(this.protos);
    if (protos.isEmpty()) {
      for (final Map.Entry<Path, Path> entry : directories.entrySet()) {
        Files.walkFileTree(entry.getValue(), new SimpleFileVisitor<Path>() {
          @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (file.getFileName().toString().endsWith(".proto")) {
              protos.add(entry.getValue().relativize(file).toString().replace(File.separator, "/"));
            }
            return FileVisitResult.CONTINUE;
          }
        });
      }
    }

    Map<String, ProtoFile> loaded = new LinkedHashMap<>();
    loaded.put(DESCRIPTOR_PROTO, loadDescriptorProto(DESCRIPTOR_PROTO));
    loaded.put(EMPTY_PROTO, loadDescriptorProto(EMPTY_PROTO));
    loaded.put(FIELD_MASK_PROTO, loadDescriptorProto(FIELD_MASK_PROTO));

    while (!protos.isEmpty()) {
      String proto = protos.removeFirst();
      if (loaded.containsKey(proto)) {
        continue;
      }

      ProtoFileElement element = null;
      for (Map.Entry<Path, Path> entry : directories.entrySet()) {
        Source source = source(entry.getValue(), proto);
        if (source == null) {
          continue;
        }

        Path base = entry.getKey();
        try {
          Location location = Location.get(base.toString(), proto);
          String data = Okio.buffer(source).readUtf8();
          element = ProtoParser.parse(location, data);
          break;
        } catch (IOException e) {
          throw new IOException("Failed to load " + proto + " from " + base, e);
        } finally {
          source.close();
        }
      }
      if (element == null) {
        throw new FileNotFoundException("Failed to locate " + proto + " in " + sources);
      }

      ProtoFile protoFile = ProtoFile.get(element);
      loaded.put(proto, protoFile);

      // Queue dependencies to be loaded.
      for (String importPath : element.getImports()) {
        protos.addLast(importPath);
      }
    }

    return new Linker(loaded.values()).link();
  }

  /**
   * Returns Google's protobuf descriptor, which defines standard options like default, deprecated,
   * and java_package. If the user has provided their own version of the descriptor proto, that is
   * preferred.
   */
  private ProtoFile loadDescriptorProto(String proto) throws IOException {
    InputStream resourceAsStream = SchemaLoader.class.getResourceAsStream("/" + proto);
    try (BufferedSource buffer = Okio.buffer(Okio.source(resourceAsStream))) {
      String data = buffer.readUtf8();
      Location location = Location.get("", proto);
      ProtoFileElement element = ProtoParser.parse(location, data);
      return ProtoFile.get(element);
    }
  }

  private static Source source(Path base, String path) throws IOException {
    Path resolvedPath = base.resolve(path);
    if (Files.exists(resolvedPath)) {
      return Okio.source(resolvedPath);
    }
    return null;
  }
}

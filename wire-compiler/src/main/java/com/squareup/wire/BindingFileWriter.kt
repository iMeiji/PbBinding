/*
 * Copyright 2018 Square Inc.
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
package com.squareup.wire

import com.squareup.kotlinpoet.*
import com.squareup.wire.kotlin.BindingGenerator
import com.squareup.wire.schema.Location
import com.squareup.wire.schema.Service
import com.squareup.wire.schema.Type
import java.io.IOException
import java.nio.file.FileSystem
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue

internal class BindingFileWriter(
    private val destination: String,
    private val bindingGenerator: BindingGenerator,
    private val queue: ConcurrentLinkedQueue<PendingTypeFileSpec>,
    private val fs: FileSystem,
    private val log: WireLogger,
    private val dryRun: Boolean
) : Callable<Unit> {

    private val postfix = "Binding"


    @Throws(IOException::class)
  override fun call() {
    while (true) {
      val pendingFile = queue.poll() ?: return
      val bindingFile = generateFileForType(pendingFile.type)

      val path = fs.getPath(destination)
      log.artifact(path, bindingFile)
      if (dryRun) return

      try {
        bindingFile.writeTo(path)
      } catch (e: IOException) {
        val className = bindingGenerator.generatedTypeName(pendingFile.type).canonicalName
        throw IOException(
            "Error emitting ${bindingFile.packageName}.$className to $destination",
            e)
      }
    }
  }

  private fun generateFileForType(type: Type): FileSpec {
    return generateFile(
        packageName = bindingGenerator.generatedTypeName(type).packageName,
        typeSpec = bindingGenerator.generateType(type),
        location = type.location()
    )
  }

  private fun generateFile(packageName: String, typeSpec: TypeSpec, location: Location?): FileSpec {
    return FileSpec.builder(packageName, typeSpec.name!!)
        .addComment(WireCompiler.CODE_GENERATED_BY_WIRE)
        .indent("    ")
        .apply {
          if (location != null) {
            addComment("\nSource file: %L", location.withPathOnly())
          }
        }
        .addType(typeSpec)
        .build()
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
description = "authorization-kafka-acl-import"

plugins {
  application
  id("java")
  id("idea")
}

// A standalone operator tool. It is deliberately not wired into copyLibAndConfig, so neither it
// nor the Gravitino Java client lands in the server's authorization plugin directory.
dependencies {
  implementation(project(":api"))
  implementation(project(":authorizations:authorization-common")) {
    exclude(group = "*")
  }
  implementation(project(":authorizations:authorization-kafka-acl"))
  implementation(project(":clients:client-java"))
  implementation(libs.commons.cli.new)
  implementation(libs.guava)
  implementation(libs.jackson.databind)
  implementation(libs.kafka.clients)
  implementation(libs.slf4j.api)
  implementation(libs.slf4j.simple)

  // The apply IT drives the real plugin, whose interface lives in core.
  testImplementation(project(":core")) {
    exclude(group = "*")
  }
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.mockito.core)
  testImplementation(libs.testcontainers)
  testImplementation(libs.testcontainers.junit.jupiter)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

application {
  mainClass.set("org.apache.gravitino.authorization.kafka.imports.KafkaAclImportTool")
}

tasks.test {
  // Unlike `hasProperty`, honor an explicit value so `-PskipITs=false` runs the ITs.
  val skipITs = project.findProperty("skipITs")?.toString()?.let { it != "false" } ?: false
  if (skipITs) {
    exclude("**/integration/test/**")
  }
}

// The parent authorizations build depends on copyLibAndConfig in every subproject. This tool must
// not be packaged into the server's plugin directory, so the task intentionally copies nothing.
tasks.register("copyLibAndConfig")

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
description = "authorization-elasticsearch"

plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api")) {
    exclude(group = "*")
  }
  implementation(project(":core")) {
    exclude(group = "*")
  }
  implementation(project(":catalogs:catalog-common")) {
    exclude(group = "*")
  }
  implementation(project(":authorizations:authorization-common")) {
    exclude(group = "*")
  }
  implementation(libs.bundles.log4j)
  implementation(libs.commons.lang3)
  implementation(libs.guava)
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)

  compileOnly(libs.lombok)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.testcontainers)
}

tasks {
  val runtimeJars by registering(Copy::class) {
    from(configurations.runtimeClasspath)
    into("build/libs")
  }

  val copyAuthorizationLibs by registering(Copy::class) {
    dependsOn("jar", runtimeJars)
    from("build/libs") {
      exclude("guava-*.jar")
      exclude("log4j-*.jar")
      exclude("slf4j-*.jar")
    }
    into("$rootDir/distribution/package/authorizations/elasticsearch/libs")
  }

  register("copyLibAndConfig", Copy::class) {
    dependsOn(copyAuthorizationLibs)
  }

  jar {
    dependsOn(runtimeJars)
  }
}

tasks.test {
  val skipITs = project.hasProperty("skipITs")
  if (skipITs) {
    exclude("**/integration/test/**")
  } else {
    dependsOn(tasks.jar)
  }
}

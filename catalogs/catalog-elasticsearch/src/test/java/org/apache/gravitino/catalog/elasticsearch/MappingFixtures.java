/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/** Loads the mapping fixtures both test classes are driven by. */
final class MappingFixtures {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private MappingFixtures() {}

  /**
   * Reads a fixture from {@code src/test/resources/mappings}.
   *
   * @param name the fixture file name, without the directory
   * @return the parsed mappings node, shaped as the mapping API returns it under {@code
   *     <index>.mappings}
   */
  static JsonNode load(String name) {
    try (InputStream fixture =
        MappingFixtures.class.getClassLoader().getResourceAsStream("mappings/" + name)) {
      if (fixture == null) {
        throw new IllegalArgumentException("No such mapping fixture: " + name);
      }
      return MAPPER.readTree(fixture);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read mapping fixture " + name, e);
    }
  }
}

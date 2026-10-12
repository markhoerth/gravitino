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
package org.apache.gravitino.authorization.kafka.imports;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/** Command line options of {@link KafkaAclImportTool}. */
final class ImportOptions {

  static final String DEFAULT_REPORT = "kafka-acl-import-report.json";

  private static final Options OPTIONS =
      new Options()
          .addOption(required("gravitino-uri", "Gravitino server URI, e.g. http://localhost:8090"))
          .addOption(required("metalake", "Metalake name"))
          .addOption(required("catalog", "Kafka catalog name"))
          .addOption(required("user", "User for Gravitino simple authentication"))
          .addOption(required("kafka-config", "Properties file passed to the Kafka AdminClient"))
          .addOption(Option.builder().longOpt("apply").desc("Write to Gravitino").build())
          .addOption(
              Option.builder()
                  .longOpt("accept-deny-removal")
                  .desc(
                      "Apply even though the plugin will delete unmapped DENY ACLs under the"
                          + " managed prefix, which widens access")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt("report")
                  .hasArg()
                  .argName("path")
                  .desc("JSON report path (default " + DEFAULT_REPORT + ")")
                  .build());

  final String gravitinoUri;
  final String metalake;
  final String catalog;
  final String user;
  final Path kafkaConfig;
  final boolean apply;
  final boolean acceptDenyRemoval;
  final Path report;

  ImportOptions(
      String gravitinoUri,
      String metalake,
      String catalog,
      String user,
      Path kafkaConfig,
      boolean apply,
      boolean acceptDenyRemoval,
      Path report) {
    this.gravitinoUri = gravitinoUri;
    this.metalake = metalake;
    this.catalog = catalog;
    this.user = user;
    this.kafkaConfig = kafkaConfig;
    this.apply = apply;
    this.acceptDenyRemoval = acceptDenyRemoval;
    this.report = report;
  }

  static ImportOptions parse(String[] args) throws ParseException {
    CommandLine cmd = new DefaultParser().parse(OPTIONS, args);
    return new ImportOptions(
        cmd.getOptionValue("gravitino-uri"),
        cmd.getOptionValue("metalake"),
        cmd.getOptionValue("catalog"),
        cmd.getOptionValue("user"),
        Paths.get(cmd.getOptionValue("kafka-config")),
        cmd.hasOption("apply"),
        cmd.hasOption("accept-deny-removal"),
        Paths.get(cmd.getOptionValue("report", DEFAULT_REPORT)));
  }

  static String usage() {
    StringWriter out = new StringWriter();
    new HelpFormatter()
        .printHelp(
            new PrintWriter(out),
            100,
            "kafka-acl-import",
            "Imports existing Kafka ACLs into Gravitino users, roles and Topic grants."
                + " Dry run unless --apply is given.",
            OPTIONS,
            2,
            2,
            "",
            true);
    return out.toString();
  }

  private static Option required(String name, String description) {
    return Option.builder().longOpt(name).hasArg().required().desc(description).build();
  }
}

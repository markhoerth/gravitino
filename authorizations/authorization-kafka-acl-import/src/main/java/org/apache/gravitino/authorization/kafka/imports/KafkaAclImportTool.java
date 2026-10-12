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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.cli.ParseException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.kafka.KafkaAclAuthorization;
import org.apache.gravitino.authorization.kafka.KafkaAclProperties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclPermissionType;

/**
 * One-time onboarding tool: reads a cluster's existing ACLs, turns the ones Gravitino can express
 * into Gravitino users, roles and Topic grants, and reports the rest with a reason. Dry run by
 * default; {@code --apply} writes to Gravitino. Nothing is ever deleted and existing roles are
 * never modified.
 */
public class KafkaAclImportTool {

  /** The schema that holds the topics of a Kafka catalog. */
  public static final String DEFAULT_SCHEMA = "default";

  /** Prefix of the role created for each imported user. */
  public static final String ROLE_PREFIX = "kafka_import_";

  /** What happened to the role of one imported user. */
  public enum RoleStatus {
    /** Dry run: the role would be created and granted. */
    PLANNED,
    /** The user, role and grant were written. */
    CREATED,
    /** The role already exists with exactly the planned contents; it was (re)granted. */
    ROLE_EXISTS_GRANTED,
    /** The role already exists with different contents; it was left untouched and not granted. */
    ROLE_EXISTS_DIFFERENT,
    /** A Gravitino call failed; see the error in the report. */
    FAILED
  }

  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  /**
   * Runs the tool and exits with 0 on success, 1 on failure and 2 on a usage error.
   *
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
    System.exit(execute(args, System.out, System.err));
  }

  @VisibleForTesting
  static int execute(String[] args, PrintStream out, PrintStream err) {
    return execute(
        args,
        out,
        err,
        o -> new GravitinoClientImportTarget(o.gravitinoUri, o.metalake, o.catalog, o.user));
  }

  @VisibleForTesting
  static int execute(
      String[] args,
      PrintStream out,
      PrintStream err,
      Function<ImportOptions, GravitinoImportTarget> targets) {
    ImportOptions options;
    try {
      options = ImportOptions.parse(args);
    } catch (ParseException e) {
      err.println(e.getMessage());
      err.println(ImportOptions.usage());
      return 2;
    }

    Properties kafkaConfig = new Properties();
    try (InputStream in = Files.newInputStream(options.kafkaConfig)) {
      kafkaConfig.load(in);
    } catch (IOException e) {
      err.println("Cannot read --kafka-config " + options.kafkaConfig + ": " + e.getMessage());
      return 1;
    }

    try (Admin admin = Admin.create(kafkaConfig);
        GravitinoImportTarget target = targets.apply(options)) {
      return run(options, admin, target, out).exitCode();
    } catch (Exception e) {
      err.println("Import failed: " + e);
      return 1;
    }
  }

  /**
   * Plans the import, applies it when requested, prints a summary and writes the JSON report.
   *
   * @param options the tool options
   * @param admin the Kafka AdminClient
   * @param target the Gravitino target
   * @param out where the summary is printed
   * @return the outcome
   * @throws Exception if Kafka or Gravitino cannot be read, or the report cannot be written
   */
  @VisibleForTesting
  static Outcome run(
      ImportOptions options, Admin admin, GravitinoImportTarget target, PrintStream out)
      throws Exception {
    Collection<AclBinding> acls = admin.describeAcls(AclBindingFilter.ANY).values().get();
    Set<String> topics = target.listTopics();
    Map<String, String> catalogProperties = target.catalogProperties();
    KafkaAclProperties props = new KafkaAclProperties(catalogProperties);
    props.validate();
    boolean pluginActive =
        KafkaAclAuthorization.SHORT_NAME.equals(
            catalogProperties.get(Catalog.AUTHORIZATION_PROVIDER));

    ImportPlan plan = KafkaAclImportPlanner.plan(acls, props, options.metalake, topics);
    Map<String, List<MappedGrant>> grantsByUser = new LinkedHashMap<>();
    for (MappedGrant grant : plan.mapped()) {
      grantsByUser.computeIfAbsent(grant.gravitinoUser(), u -> new ArrayList<>()).add(grant);
    }
    List<ObjectNode> roles = new ArrayList<>();
    for (Map.Entry<String, List<MappedGrant>> entry : grantsByUser.entrySet()) {
      ObjectNode node =
          MAPPER
              .createObjectNode()
              .put("user", entry.getKey())
              .put("role", roleName(entry.getKey()))
              .put("status", RoleStatus.PLANNED.name());
      node.set("topics", MAPPER.valueToTree(topicsOf(entry.getValue())));
      roles.add(node);
    }

    ApplyResult result = new ApplyResult();
    List<AclBinding> denyRemovals = pluginDeleteDenies(plan, props);
    if (options.apply && !denyRemovals.isEmpty() && !options.acceptDenyRemoval) {
      out.printf(
          "REFUSED: the plugin would delete %d unmapped DENY ACL(s) under %s, widening access."
              + " Remove or replace them first, or pass --accept-deny-removal:%n",
          denyRemovals.size(), props.principalPrefix());
      denyRemovals.forEach(acl -> out.println("  " + acl));
      result.status = "REFUSED";
      result.exitCode = 2;
    } else if (options.apply) {
      apply(options, target, grantsByUser, roles, result, out);
      // Every hook has run by now; check the cluster kept what we imported.
      Set<AclBinding> now = new HashSet<>(admin.describeAcls(AclBindingFilter.ANY).values().get());
      List<AclBinding> expected = new ArrayList<>(plan.implied());
      plan.mapped().forEach(g -> expected.add(g.source()));
      result.missing =
          expected.stream()
              .filter(b -> !now.contains(b))
              .sorted(KafkaAclImportPlanner.ACL_ORDER)
              .collect(Collectors.toList());
      if (!result.missing.isEmpty()) {
        result.fail();
      }
    }

    ObjectNode report = report(options, props, pluginActive, acls.size(), plan, roles, result);
    Files.write(options.report, MAPPER.writeValueAsBytes(report));
    printSummary(out, options, props, pluginActive, acls.size(), plan, roles, result);
    out.println("Report written to " + options.report.toAbsolutePath());
    return new Outcome(result.exitCode, report);
  }

  /**
   * Pauses the plugin, writes users, roles and grants, always unpauses (also on failure and on JVM
   * shutdown), then fires one hook so the plugin reconciles once against the final state.
   */
  private static void apply(
      ImportOptions options,
      GravitinoImportTarget target,
      Map<String, List<MappedGrant>> grantsByUser,
      List<ObjectNode> roles,
      ApplyResult result,
      PrintStream out) {
    AtomicBoolean unpaused = new AtomicBoolean(false);
    Runnable unpause =
        () -> {
          if (unpaused.compareAndSet(false, true)) {
            target.setCatalogProperty(KafkaAclProperties.RECONCILE_PAUSED, "false");
          }
        };
    Thread shutdownHook = new Thread(unpause, "kafka-acl-import-unpause");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    try {
      target.setCatalogProperty(KafkaAclProperties.RECONCILE_PAUSED, "true");
      for (ObjectNode node : roles) {
        String user = node.get("user").asText();
        String role = node.get("role").asText();
        try {
          node.put(
              "status",
              applyUser(target, options.catalog, user, role, grantsByUser.get(user)).name());
        } catch (RuntimeException e) {
          node.put("status", RoleStatus.FAILED.name()).put("error", e.toString());
          result.fail();
        }
      }
    } finally {
      try {
        unpause.run();
        result.unpaused = true;
      } catch (RuntimeException e) {
        result.fail();
        result.errors.add(
            "Failed to clear "
                + KafkaAclProperties.RECONCILE_PAUSED
                + "; the plugin stays paused until it is set to false: "
                + e);
      }
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException e) {
        // The JVM is already shutting down and runs the hook itself.
      }
    }

    if (result.unpaused) {
      try {
        result.reconcileTrigger = target.triggerReconcile();
      } catch (RuntimeException e) {
        result.fail();
        result.errors.add("Failed to trigger a reconcile after unpausing: " + e);
      }
    }
    result.errors.forEach(e -> out.println("ERROR: " + e));
  }

  /**
   * The role name for an imported user: {@code kafka_import_}, the user name with every character
   * outside {@code [A-Za-z0-9_]} replaced by {@code _}, then {@code _} and the first 8 hex digits
   * of the SHA-256 of the original user name, so that names that sanitize alike cannot collide.
   *
   * @param user the Gravitino user name
   * @return the role name
   */
  public static String roleName(String user) {
    String hash = Hashing.sha256().hashString(user, StandardCharsets.UTF_8).toString();
    return ROLE_PREFIX + user.replaceAll("[^A-Za-z0-9_]", "_") + "_" + hash.substring(0, 8);
  }

  /**
   * Builds the securable objects of an imported user's role.
   *
   * @param catalog the catalog name
   * @param grants the user's mapped grants
   * @return USE_CATALOG, USE_SCHEMA on {@code default}, and one object per topic
   */
  public static List<SecurableObject> securableObjects(
      String catalog, Collection<MappedGrant> grants) {
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(catalog, ImmutableList.of(Privileges.UseCatalog.allow()));
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, DEFAULT_SCHEMA, ImmutableList.of(Privileges.UseSchema.allow()));
    Map<String, List<Privilege>> byTopic = new LinkedHashMap<>();
    grants.stream()
        .sorted(
            Comparator.comparing(MappedGrant::topic)
                .thenComparing(MappedGrant::name)
                .thenComparing(MappedGrant::deny))
        .forEach(
            g ->
                byTopic
                    .computeIfAbsent(g.topic(), t -> new ArrayList<>())
                    .add(g.deny() ? Privileges.deny(g.name()) : Privileges.allow(g.name())));
    List<SecurableObject> objects = new ArrayList<>();
    objects.add(catalogObject);
    objects.add(schemaObject);
    byTopic.forEach(
        (topic, privileges) ->
            objects.add(SecurableObjects.ofTopic(schemaObject, topic, privileges)));
    return objects;
  }

  private static RoleStatus applyUser(
      GravitinoImportTarget target,
      String catalog,
      String user,
      String role,
      List<MappedGrant> grants) {
    List<SecurableObject> planned = securableObjects(catalog, grants);
    Optional<List<SecurableObject>> existing = target.roleSecurableObjects(role);
    if (existing.isPresent() && !normalize(existing.get()).equals(normalize(planned))) {
      return RoleStatus.ROLE_EXISTS_DIFFERENT;
    }
    if (!target.userExists(user)) {
      target.addUser(user);
    }
    if (existing.isPresent()) {
      target.grantRoleToUser(role, user);
      return RoleStatus.ROLE_EXISTS_GRANTED;
    }
    target.createRole(role, planned);
    target.grantRoleToUser(role, user);
    return RoleStatus.CREATED;
  }

  /** Compares roles by object type, full name and the set of (condition, privilege) pairs. */
  private static Set<String> normalize(List<SecurableObject> objects) {
    Set<String> normalized = new TreeSet<>();
    for (SecurableObject object : objects) {
      Set<String> privileges = new TreeSet<>();
      object.privileges().forEach(p -> privileges.add(p.condition() + " " + p.name()));
      normalized.add(object.type() + " " + object.fullName() + " " + privileges);
    }
    return normalized;
  }

  private static Set<String> topicsOf(List<MappedGrant> grants) {
    return grants.stream().map(MappedGrant::topic).collect(Collectors.toCollection(TreeSet::new));
  }

  private static ObjectNode report(
      ImportOptions options,
      KafkaAclProperties props,
      boolean pluginActive,
      int aclCount,
      ImportPlan plan,
      List<ObjectNode> roles,
      ApplyResult result) {
    ObjectNode report = MAPPER.createObjectNode();
    report
        .put("mode", options.apply ? "apply" : "dry-run")
        .put("status", result.status)
        .put("metalake", options.metalake)
        .put("catalog", options.catalog)
        .put("principalPrefix", props.principalPrefix())
        .put("groupPrefixTemplate", props.groupPrefixTemplate())
        .put("pluginActive", pluginActive);

    ObjectNode summary = report.putObject("summary");
    summary
        .put("acls", aclCount)
        .put("mapped", plan.mapped().size())
        .put("implied", plan.implied().size())
        .put("unmapped", plan.unmapped().size())
        .put("pluginWillCreate", plan.additions().size())
        .put("pluginWillDelete", pluginDeletes(plan, props).size())
        .put("pluginWillDeleteDeny", pluginDeleteDenies(plan, props).size());
    ObjectNode byReason = summary.putObject("unmappedByReason");
    reasonCounts(plan).forEach((reason, count) -> byReason.put(reason.name(), count));

    ArrayNode mapped = report.putArray("mapped");
    for (MappedGrant g : plan.mapped()) {
      mapped
          .addObject()
          .put("user", g.gravitinoUser())
          .put("topic", g.topic())
          .put("privilege", g.name().name())
          .put("effect", g.deny() ? "DENY" : "ALLOW")
          .set("acl", acl(g.source()));
    }
    ArrayNode implied = report.putArray("implied");
    plan.implied().forEach(b -> implied.add(acl(b)));
    ArrayNode unmapped = report.putArray("unmapped");
    for (UnmappedAcl u : plan.unmapped()) {
      unmapped
          .addObject()
          .put("reason", u.reason().name())
          .put("deletedByPlugin", ownedBy(props, u.acl()))
          .set("acl", acl(u.acl()));
    }
    ArrayNode additions = report.putArray("pluginWillCreate");
    plan.additions().forEach(b -> additions.add(acl(b)));
    report.putArray("roles").addAll(roles);
    ArrayNode denies = report.putArray("pluginWillDeleteDeny");
    pluginDeleteDenies(plan, props).forEach(b -> denies.add(acl(b)));
    if (options.apply) {
      report.put("acceptDenyRemoval", options.acceptDenyRemoval);
      report.put("unpaused", result.unpaused);
      report.put("reconcileTrigger", result.reconcileTrigger);
      ArrayNode missingNode = report.putArray("missingAfterApply");
      result.missing.forEach(b -> missingNode.add(acl(b)));
      ArrayNode errors = report.putArray("errors");
      result.errors.forEach(errors::add);
    }
    return report;
  }

  private static void printSummary(
      PrintStream out,
      ImportOptions options,
      KafkaAclProperties props,
      boolean pluginActive,
      int aclCount,
      ImportPlan plan,
      List<ObjectNode> roles,
      ApplyResult result) {
    out.printf(
        "Kafka ACL import (%s) metalake=%s catalog=%s%n",
        options.apply ? "apply" : "dry run", options.metalake, options.catalog);
    row(out, "ACLs on cluster", aclCount);
    row(out, "mapped (become grants)", plan.mapped().size());
    row(out, "implied (regenerated)", plan.implied().size());
    row(out, "unmapped", plan.unmapped().size());
    reasonCounts(plan).forEach((reason, count) -> row(out, "  " + reason, count));
    row(out, "plugin will create", plan.additions().size());
    row(out, "plugin will delete", pluginDeletes(plan, props).size());
    int denyDeletes = pluginDeleteDenies(plan, props).size();
    if (denyDeletes > 0) {
      out.printf("WARNING: %d of those are DENY ACLs; deleting them widens access.%n", denyDeletes);
    }
    if (pluginActive) {
      out.println(
          "The kafka-acl plugin is active on this catalog; --apply pauses its reconcile while"
              + " importing and triggers one reconcile afterwards.");
    }
    out.println("Roles:");
    for (ObjectNode role : roles) {
      out.printf(
          "  %-40s %-40s %-12s %s%n",
          role.get("user").asText(),
          role.get("role").asText(),
          role.get("status").asText(),
          role.get("topics"));
    }
    if (options.apply && result.unpaused) {
      out.println("Reconcile triggered via " + result.reconcileTrigger);
      row(out, "imported ACLs missing after apply", result.missing.size());
    }
  }

  private static void row(PrintStream out, String label, long count) {
    out.printf("  %-36s %6d%n", label, count);
  }

  private static Map<UnmappedAcl.Reason, Long> reasonCounts(ImportPlan plan) {
    Map<UnmappedAcl.Reason, Long> counts = new EnumMap<>(UnmappedAcl.Reason.class);
    plan.unmapped().forEach(u -> counts.merge(u.reason(), 1L, Long::sum));
    return counts;
  }

  private static List<AclBinding> pluginDeletes(ImportPlan plan, KafkaAclProperties props) {
    return plan.unmapped().stream()
        .map(UnmappedAcl::acl)
        .filter(acl -> ownedBy(props, acl))
        .collect(Collectors.toList());
  }

  private static List<AclBinding> pluginDeleteDenies(ImportPlan plan, KafkaAclProperties props) {
    return pluginDeletes(plan, props).stream()
        .filter(acl -> acl.entry().permissionType() == AclPermissionType.DENY)
        .collect(Collectors.toList());
  }

  private static boolean ownedBy(KafkaAclProperties props, AclBinding acl) {
    return acl.entry().principal().startsWith(props.principalPrefix());
  }

  private static ObjectNode acl(AclBinding b) {
    return MAPPER
        .createObjectNode()
        .put("resourceType", b.pattern().resourceType().name())
        .put("name", b.pattern().name())
        .put("patternType", b.pattern().patternType().name())
        .put("principal", b.entry().principal())
        .put("host", b.entry().host())
        .put("operation", b.entry().operation().name())
        .put("permission", b.entry().permissionType().name());
  }

  /** Mutable state of one --apply. */
  private static final class ApplyResult {
    private String status = "OK";
    private int exitCode = 0;
    private boolean unpaused;
    private String reconcileTrigger;
    private List<AclBinding> missing = ImmutableList.of();
    private final List<String> errors = new ArrayList<>();

    private void fail() {
      status = "FAILED";
      exitCode = 1;
    }
  }

  /** The result of one run. */
  static final class Outcome {
    private final int exitCode;
    private final ObjectNode report;

    Outcome(int exitCode, ObjectNode report) {
      this.exitCode = exitCode;
      this.report = report;
    }

    int exitCode() {
      return exitCode;
    }

    boolean succeeded() {
      return exitCode == 0;
    }

    ObjectNode report() {
      return report;
    }
  }
}

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
package org.apache.gravitino.authorization.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates a Gravitino role into an Elasticsearch role descriptor.
 *
 * <p>Elasticsearch has no catalog or schema, only a flat namespace of indices, so the Gravitino
 * hierarchy is flattened into index names and patterns. Elasticsearch privileges are also purely
 * additive, so a Gravitino deny is realised by expanding the pattern it sits under into an explicit
 * list of the indices that remain allowed.
 *
 * <p>Row filters and column restrictions are read from role properties in this prototype:
 *
 * <ul>
 *   <li>{@code elasticsearch.dls.<index>} holds a Query DSL object applied as document-level
 *       security, and {@code elasticsearch.dls.*} applies to every index in the role.
 *   <li>{@code elasticsearch.fls.<index>} holds a comma-separated list of fields to hide, and
 *       {@code elasticsearch.fls.*} applies to every index in the role.
 * </ul>
 */
public class ElasticsearchRoleTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchRoleTranslator.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Role property prefix carrying a document-level security query. */
  public static final String DLS_PROPERTY_PREFIX = "elasticsearch.dls.";

  /** Role property prefix carrying the fields hidden by field-level security. */
  public static final String FLS_PROPERTY_PREFIX = "elasticsearch.fls.";

  /** Property key suffix that applies a restriction to every index in the role. */
  public static final String ALL_INDICES = "*";

  private final String indexPrefix;
  private final Supplier<List<String>> indexLister;
  private final boolean requireUsagePrivileges;

  /**
   * Creates a translator.
   *
   * @param indexPrefix the prefix scoping catalog and schema level grants.
   * @param indexLister supplies the concrete index names, used to expand a deny into an allow list.
   */
  public ElasticsearchRoleTranslator(String indexPrefix, Supplier<List<String>> indexLister) {
    this(indexPrefix, indexLister, true);
  }

  /**
   * Creates a translator.
   *
   * @param indexPrefix the prefix scoping catalog and schema level grants.
   * @param indexLister supplies the concrete index names, used to expand a deny into an allow list.
   * @param requireUsagePrivileges whether data privileges require USE_CATALOG and USE_SCHEMA in the
   *     same role, matching how Gravitino gates access to a table.
   */
  public ElasticsearchRoleTranslator(
      String indexPrefix, Supplier<List<String>> indexLister, boolean requireUsagePrivileges) {
    this.indexPrefix = indexPrefix == null ? "" : indexPrefix;
    this.indexLister = indexLister;
    this.requireUsagePrivileges = requireUsagePrivileges;
  }

  /**
   * Returns whether a role carries the usage privileges that Gravitino requires before a data
   * privilege grants anything.
   *
   * <p>Gravitino's table expression reads {@code ANY_USE_CATALOG && ANY_USE_SCHEMA &&
   * (ANY_SELECT_TABLE || ...)}, where USE_CATALOG counts at metalake or catalog level and
   * USE_SCHEMA at metalake, catalog or schema level. A role holding only SELECT_TABLE therefore
   * grants nothing in Gravitino, and must grant nothing in Elasticsearch either.
   */
  private boolean hasUsagePrivileges(Role role) {
    boolean useCatalog = false;
    boolean useSchema = false;

    for (SecurableObject object : nullSafe(role.securableObjects())) {
      MetadataObject.Type type = object.type();
      for (Privilege privilege : nullSafe(object.privileges())) {
        if (privilege.condition() == Privilege.Condition.DENY) {
          continue;
        }
        if (privilege.name() == Privilege.Name.USE_CATALOG
            && (type == MetadataObject.Type.METALAKE || type == MetadataObject.Type.CATALOG)) {
          useCatalog = true;
        }
        if (privilege.name() == Privilege.Name.USE_SCHEMA
            && (type == MetadataObject.Type.METALAKE
                || type == MetadataObject.Type.CATALOG
                || type == MetadataObject.Type.SCHEMA)) {
          useSchema = true;
        }
      }
    }
    return useCatalog && useSchema;
  }

  /**
   * Translates a Gravitino role into an Elasticsearch role descriptor.
   *
   * @param role the Gravitino role.
   * @param metalake the metalake owning the role, recorded in the descriptor metadata.
   * @return the Elasticsearch role descriptor.
   */
  public ObjectNode translate(Role role, String metalake) {
    Map<String, Set<String>> allowed = new LinkedHashMap<>();
    Map<String, Set<String>> denied = new LinkedHashMap<>();
    boolean usageSatisfied = !requireUsagePrivileges || hasUsagePrivileges(role);

    for (SecurableObject object : nullSafe(role.securableObjects())) {
      String pattern = indexPattern(object);
      if (pattern == null) {
        continue;
      }
      for (Privilege privilege : nullSafe(object.privileges())) {
        if (!usageSatisfied
            && privilege.condition() != Privilege.Condition.DENY
            && ElasticsearchPrivileges.requiresUsage(privilege.name())) {
          LOG.warn(
              "Role {} carries {} on {} without USE_CATALOG and USE_SCHEMA. Gravitino refuses "
                  + "access in that case, so the privilege is not pushed to Elasticsearch.",
              role.name(),
              privilege.name(),
              object.fullName());
          continue;
        }
        Set<String> esPrivileges = ElasticsearchPrivileges.indexPrivileges(privilege.name());
        if (esPrivileges.isEmpty()) {
          LOG.debug(
              "Privilege {} on {} has no Elasticsearch counterpart and is skipped",
              privilege.name(),
              object.fullName());
          continue;
        }
        Map<String, Set<String>> target =
            privilege.condition() == Privilege.Condition.DENY ? denied : allowed;
        target.computeIfAbsent(pattern, key -> new LinkedHashSet<>()).addAll(esPrivileges);
      }
    }

    Map<String, Set<String>> resolved = resolveDenies(allowed, denied);

    ObjectNode descriptor = MAPPER.createObjectNode();
    descriptor.putArray("cluster");
    ArrayNode indices = descriptor.putArray("indices");
    resolved.forEach((names, privileges) -> indices.add(indexEntry(names, privileges, role)));

    ObjectNode metadata = descriptor.putObject("metadata");
    metadata.put("managed_by", "gravitino");
    metadata.put("gravitino_metalake", metalake);
    metadata.put("gravitino_role", role.name());
    return descriptor;
  }

  /**
   * Returns the index pattern a securable object maps to, or null when the object type has no
   * Elasticsearch counterpart.
   *
   * @param object the securable object.
   * @return the index name or pattern.
   */
  private String indexPattern(SecurableObject object) {
    MetadataObject.Type type = object.type();
    if (type == MetadataObject.Type.METALAKE
        || type == MetadataObject.Type.CATALOG
        || type == MetadataObject.Type.SCHEMA) {
      // One catalog is one Elasticsearch cluster and the schema is synthetic, so both widen to
      // every index the catalog covers.
      return indexPrefix + "*";
    }
    if (type == MetadataObject.Type.TABLE || type == MetadataObject.Type.VIEW) {
      return object.name();
    }
    LOG.debug("Securable object type {} has no Elasticsearch counterpart", type);
    return null;
  }

  /**
   * Realises denies against allows. Elasticsearch roles are additive, so a deny that falls under an
   * allowed pattern is realised by replacing the pattern with the indices that remain allowed. A
   * deny of a literal index simply removes that index entry.
   */
  private Map<String, Set<String>> resolveDenies(
      Map<String, Set<String>> allowed, Map<String, Set<String>> denied) {
    if (denied.isEmpty()) {
      return allowed;
    }

    Map<String, Set<String>> resolved = new LinkedHashMap<>();
    List<String> clusterIndices = null;

    for (Map.Entry<String, Set<String>> entry : allowed.entrySet()) {
      String pattern = entry.getKey();
      Set<String> privileges = entry.getValue();

      Set<String> overlappingDenies =
          denied.entrySet().stream()
              .filter(d -> matches(pattern, d.getKey()) && intersects(privileges, d.getValue()))
              .map(Map.Entry::getKey)
              .collect(Collectors.toCollection(LinkedHashSet::new));

      if (overlappingDenies.isEmpty()) {
        resolved.computeIfAbsent(pattern, key -> new LinkedHashSet<>()).addAll(privileges);
        continue;
      }

      if (!isPattern(pattern)) {
        // A literal index denied outright contributes nothing.
        continue;
      }

      if (clusterIndices == null) {
        clusterIndices = indexLister.get();
      }
      List<String> remaining =
          clusterIndices.stream()
              .filter(index -> matches(pattern, index))
              .filter(index -> overlappingDenies.stream().noneMatch(d -> matches(d, index)))
              .collect(Collectors.toList());

      LOG.info(
          "Expanded pattern {} into {} explicit indices because a deny sits below it. Indices "
              + "created after this point stay out of the role until the next reconciliation.",
          pattern,
          remaining.size());

      for (String index : remaining) {
        resolved.computeIfAbsent(index, key -> new LinkedHashSet<>()).addAll(privileges);
      }
    }
    return resolved;
  }

  private ObjectNode indexEntry(String names, Set<String> privileges, Role role) {
    ObjectNode entry = MAPPER.createObjectNode();
    entry.putArray("names").add(names);
    ArrayNode privilegeArray = entry.putArray("privileges");
    privileges.forEach(privilegeArray::add);

    boolean readsDocuments = privileges.stream().anyMatch(ElasticsearchPrivileges::isReadPrivilege);
    if (!readsDocuments) {
      return entry;
    }

    Map<String, String> properties =
        role.properties() == null ? java.util.Collections.emptyMap() : role.properties();

    String dls =
        firstNonBlank(
            properties.get(DLS_PROPERTY_PREFIX + names),
            properties.get(DLS_PROPERTY_PREFIX + ALL_INDICES));
    if (StringUtils.isNotBlank(dls)) {
      try {
        entry.set("query", MAPPER.readTree(dls));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Role "
                + role.name()
                + " carries a document-level security query for "
                + names
                + " that is not valid JSON: "
                + dls,
            e);
      }
    }

    String fls =
        firstNonBlank(
            properties.get(FLS_PROPERTY_PREFIX + names),
            properties.get(FLS_PROPERTY_PREFIX + ALL_INDICES));
    if (StringUtils.isNotBlank(fls)) {
      ObjectNode fieldSecurity = entry.putObject("field_security");
      fieldSecurity.putArray("grant").add("*");
      ArrayNode except = fieldSecurity.putArray("except");
      Arrays.stream(fls.split(","))
          .map(String::trim)
          .filter(StringUtils::isNotBlank)
          .forEach(except::add);
    }

    if ((StringUtils.isNotBlank(dls) || StringUtils.isNotBlank(fls))
        && privileges.contains(ElasticsearchPrivileges.WRITE)) {
      throw new IllegalArgumentException(
          "Role "
              + role.name()
              + " carries both write access and a row or column restriction on "
              + names
              + ". Elasticsearch restricts document updates on indices under document- or "
              + "field-level security, so the two cannot be combined in one role.");
    }
    return entry;
  }

  private static String firstNonBlank(String first, String second) {
    return StringUtils.isNotBlank(first) ? first : second;
  }

  private static boolean isPattern(String value) {
    return value.contains("*") || value.contains("?");
  }

  /** Returns whether an index name or pattern falls within another pattern. */
  private static boolean matches(String pattern, String candidate) {
    if (pattern.equals(candidate)) {
      return true;
    }
    if (!isPattern(pattern)) {
      return false;
    }
    return candidate.matches(globToRegex(pattern));
  }

  /** Converts an Elasticsearch index glob into a regular expression. */
  private static String globToRegex(String glob) {
    StringBuilder regex = new StringBuilder();
    StringBuilder literal = new StringBuilder();
    for (int i = 0; i < glob.length(); i++) {
      char c = glob.charAt(i);
      if (c == '*' || c == '?') {
        if (literal.length() > 0) {
          regex.append(java.util.regex.Pattern.quote(literal.toString()));
          literal.setLength(0);
        }
        regex.append(c == '*' ? ".*" : ".");
      } else {
        literal.append(c);
      }
    }
    if (literal.length() > 0) {
      regex.append(java.util.regex.Pattern.quote(literal.toString()));
    }
    return regex.toString();
  }

  private static boolean intersects(Set<String> left, Set<String> right) {
    return left.stream().anyMatch(right::contains);
  }

  private static <T> Collection<T> nullSafe(Collection<T> collection) {
    return collection == null ? new ArrayList<>() : collection;
  }
}

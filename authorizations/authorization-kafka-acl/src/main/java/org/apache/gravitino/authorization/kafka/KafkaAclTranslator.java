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
package org.apache.gravitino.authorization.kafka;

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates effective Gravitino Topic grants into Kafka ACL bindings. Pure: no I/O.
 *
 * <ul>
 *   <li>CONSUME_TOPIC ALLOW: Topic READ + DESCRIBE ALLOW
 *   <li>CONSUME_TOPIC DENY: Topic READ + DESCRIBE DENY
 *   <li>PRODUCE_TOPIC ALLOW: Topic WRITE + DESCRIBE ALLOW
 *   <li>PRODUCE_TOPIC DENY: Topic WRITE DENY
 *   <li>any effective CONSUME_TOPIC ALLOW: Group READ on the Principal's PREFIXED group prefix
 *   <li>CREATE_TOPIC: nothing
 * </ul>
 *
 * Topic bindings are LITERAL. Catalog, schema and metalake level CONSUME/PRODUCE grants are
 * rejected, and no wildcard resource is ever emitted.
 */
public class KafkaAclTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAclTranslator.class);

  /** Host value of the ACL entries; the plugin does not restrict client hosts. */
  private static final String ANY_HOST = "*";

  private static final Set<AclOperation> CONSUME =
      ImmutableSet.of(AclOperation.READ, AclOperation.DESCRIBE);
  private static final Set<AclOperation> PRODUCE_ALLOW =
      ImmutableSet.of(AclOperation.WRITE, AclOperation.DESCRIBE);
  private static final Set<AclOperation> PRODUCE_DENY = ImmutableSet.of(AclOperation.WRITE);

  private final String metalake;
  private final String groupPrefixTemplate;

  /**
   * Creates a translator.
   *
   * @param metalake the metalake name, substituted into the group prefix template
   * @param groupPrefixTemplate the consumer group prefix template
   */
  public KafkaAclTranslator(String metalake, String groupPrefixTemplate) {
    this.metalake = metalake;
    this.groupPrefixTemplate = groupPrefixTemplate;
  }

  /**
   * Translates grants into ACL bindings. DENY is applied over ALLOW first.
   *
   * @param grants the grants of all Principals
   * @return the desired ACL bindings
   * @throws AuthorizationPluginException if a grant cannot be expressed as literal ACLs
   */
  public Set<AclBinding> translate(Collection<KafkaGrant> grants) {
    Set<AclBinding> bindings = new HashSet<>();
    Set<String> consumers = new HashSet<>();

    for (KafkaGrant grant : KafkaGrant.effective(grants)) {
      Set<AclOperation> operations;
      switch (grant.privilege()) {
        case CONSUME_TOPIC:
          operations = CONSUME;
          break;
        case PRODUCE_TOPIC:
          operations = grant.effect() == KafkaGrant.Effect.ALLOW ? PRODUCE_ALLOW : PRODUCE_DENY;
          break;
        case CREATE_TOPIC:
          LOG.info("Skip {}: CREATE_TOPIC is enforced by Gravitino, not pushed to Kafka", grant);
          continue;
        default:
          LOG.debug("Skip {}: privilege has no Kafka ACL mapping", grant);
          continue;
      }

      if (grant.scope() != KafkaGrant.Scope.TOPIC) {
        throw new AuthorizationPluginException(
            "Cannot push down %s: catalog-level Kafka grants need an explicit wildcard or prefix"
                + " mapping; not supported yet",
            grant);
      }
      rejectWildcard(grant.resource(), grant);

      AclPermissionType permission =
          grant.effect() == KafkaGrant.Effect.ALLOW
              ? AclPermissionType.ALLOW
              : AclPermissionType.DENY;
      ResourcePattern topic =
          new ResourcePattern(ResourceType.TOPIC, grant.resource(), PatternType.LITERAL);
      for (AclOperation operation : operations) {
        bindings.add(
            new AclBinding(
                topic, new AccessControlEntry(grant.principal(), ANY_HOST, operation, permission)));
      }

      if (grant.privilege() == Privilege.Name.CONSUME_TOPIC
          && grant.effect() == KafkaGrant.Effect.ALLOW) {
        consumers.add(grant.principal());
      }
    }

    for (String principal : consumers) {
      String groupPrefix = groupPrefix(principal);
      rejectWildcard(groupPrefix, principal);
      bindings.add(
          new AclBinding(
              new ResourcePattern(ResourceType.GROUP, groupPrefix, PatternType.PREFIXED),
              new AccessControlEntry(
                  principal, ANY_HOST, AclOperation.READ, AclPermissionType.ALLOW)));
    }
    return bindings;
  }

  /**
   * Builds the consumer group prefix of a Principal from the template.
   *
   * @param principal the Kafka Principal, e.g. {@code User:flink.order_enricher}
   * @return the group prefix, e.g. {@code flink.analytics.flink.order_enricher.}
   */
  public String groupPrefix(String principal) {
    return groupPrefixTemplate
        .replace(KafkaAclProperties.METALAKE_PLACEHOLDER, metalake)
        .replace(
            KafkaAclProperties.PRINCIPAL_PLACEHOLDER,
            KafkaPrincipalResolver.principalName(principal));
  }

  private static void rejectWildcard(String resourceName, Object source) {
    if (resourceName.isEmpty() || ResourcePattern.WILDCARD_RESOURCE.equals(resourceName)) {
      throw new AuthorizationPluginException(
          "Refusing to emit a wildcard Kafka ACL resource '%s' for %s", resourceName, source);
    }
  }
}

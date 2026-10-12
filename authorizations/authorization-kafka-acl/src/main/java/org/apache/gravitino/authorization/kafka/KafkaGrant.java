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

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.Privilege;

/**
 * One effective grant of a Gravitino Topic privilege to a Kafka Principal. This is the input of
 * {@link KafkaAclTranslator}.
 */
public final class KafkaGrant {

  /** The scope of the securable object the privilege was granted on. */
  public enum Scope {
    /** A single topic; the resource name is the Kafka topic name. */
    TOPIC,
    /** A schema; the resource name is the Gravitino full name. */
    SCHEMA,
    /** A catalog; the resource name is the catalog name. */
    CATALOG,
    /** A metalake; the resource name is the metalake name. */
    METALAKE
  }

  /** Whether the grant allows or denies the privilege. */
  public enum Effect {
    /** The privilege is allowed. */
    ALLOW,
    /** The privilege is denied. DENY wins over ALLOW. */
    DENY
  }

  private final String principal;
  private final Scope scope;
  private final String resource;
  private final Privilege.Name privilege;
  private final Effect effect;

  /**
   * Creates a grant.
   *
   * @param principal the Kafka Principal, e.g. {@code User:flink.order_enricher}
   * @param scope the scope of the securable object
   * @param resource the topic name for {@link Scope#TOPIC}, the Gravitino full name otherwise
   * @param privilege the Gravitino privilege
   * @param effect ALLOW or DENY
   */
  public KafkaGrant(
      String principal, Scope scope, String resource, Privilege.Name privilege, Effect effect) {
    this.principal = Preconditions.checkNotNull(principal, "principal");
    this.scope = Preconditions.checkNotNull(scope, "scope");
    this.resource = Preconditions.checkNotNull(resource, "resource");
    this.privilege = Preconditions.checkNotNull(privilege, "privilege");
    this.effect = Preconditions.checkNotNull(effect, "effect");
  }

  /**
   * Creates a topic-level grant.
   *
   * @param principal the Kafka Principal
   * @param topic the Kafka topic name
   * @param privilege the Gravitino privilege
   * @param effect ALLOW or DENY
   * @return the grant
   */
  public static KafkaGrant topic(
      String principal, String topic, Privilege.Name privilege, Effect effect) {
    return new KafkaGrant(principal, Scope.TOPIC, topic, privilege, effect);
  }

  /**
   * Resolves DENY over ALLOW: drops every ALLOW grant for which a DENY grant exists with the same
   * Principal, scope, resource and privilege. The result is the union of ALLOW minus DENY, plus the
   * DENY grants themselves.
   *
   * @param grants the raw grants collected across all roles of all Principals
   * @return the effective grants
   */
  public static Set<KafkaGrant> effective(Collection<KafkaGrant> grants) {
    Set<KafkaGrant> denies =
        grants.stream().filter(g -> g.effect == Effect.DENY).collect(Collectors.toSet());
    return grants.stream()
        .filter(g -> g.effect == Effect.DENY || !denies.contains(g.withEffect(Effect.DENY)))
        .collect(Collectors.toSet());
  }

  /**
   * @return the Kafka Principal, e.g. {@code User:flink.order_enricher}
   */
  public String principal() {
    return principal;
  }

  /**
   * @return the scope of the securable object
   */
  public Scope scope() {
    return scope;
  }

  /**
   * @return the topic name for {@link Scope#TOPIC}, the Gravitino full name otherwise
   */
  public String resource() {
    return resource;
  }

  /**
   * @return the Gravitino privilege
   */
  public Privilege.Name privilege() {
    return privilege;
  }

  /**
   * @return ALLOW or DENY
   */
  public Effect effect() {
    return effect;
  }

  private KafkaGrant withEffect(Effect newEffect) {
    return new KafkaGrant(principal, scope, resource, privilege, newEffect);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaGrant)) {
      return false;
    }
    KafkaGrant that = (KafkaGrant) o;
    return principal.equals(that.principal)
        && scope == that.scope
        && resource.equals(that.resource)
        && privilege == that.privilege
        && effect == that.effect;
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, scope, resource, privilege, effect);
  }

  @Override
  public String toString() {
    return String.format("%s %s %s on %s %s", effect, privilege, principal, scope, resource);
  }
}

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

import java.util.Objects;
import org.apache.gravitino.authorization.Privilege;
import org.apache.kafka.common.acl.AclBinding;

/** An existing Kafka ACL that maps to one Gravitino Topic privilege. */
public final class MappedGrant {

  private final String gravitinoUser;
  private final String topic;
  private final Privilege.Name name;
  private final boolean deny;
  private final AclBinding source;

  /**
   * Creates a mapped grant.
   *
   * @param gravitinoUser the Gravitino user name (the Kafka Principal without {@code User:})
   * @param topic the Kafka topic name
   * @param name CONSUME_TOPIC or PRODUCE_TOPIC
   * @param deny whether the privilege is denied
   * @param source the ACL this grant was mapped from
   */
  public MappedGrant(
      String gravitinoUser, String topic, Privilege.Name name, boolean deny, AclBinding source) {
    this.gravitinoUser = gravitinoUser;
    this.topic = topic;
    this.name = name;
    this.deny = deny;
    this.source = source;
  }

  /**
   * @return the Gravitino user name
   */
  public String gravitinoUser() {
    return gravitinoUser;
  }

  /**
   * @return the Kafka topic name
   */
  public String topic() {
    return topic;
  }

  /**
   * @return the Gravitino privilege
   */
  public Privilege.Name name() {
    return name;
  }

  /**
   * @return whether the privilege is denied
   */
  public boolean deny() {
    return deny;
  }

  /**
   * @return the ACL this grant was mapped from
   */
  public AclBinding source() {
    return source;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MappedGrant)) {
      return false;
    }
    MappedGrant that = (MappedGrant) o;
    return deny == that.deny
        && gravitinoUser.equals(that.gravitinoUser)
        && topic.equals(that.topic)
        && name == that.name
        && source.equals(that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(gravitinoUser, topic, name, deny, source);
  }

  @Override
  public String toString() {
    return String.format("%s %s %s on %s", deny ? "DENY" : "ALLOW", name, gravitinoUser, topic);
  }
}

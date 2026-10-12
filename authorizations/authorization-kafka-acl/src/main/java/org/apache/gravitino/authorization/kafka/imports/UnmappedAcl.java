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
import org.apache.kafka.common.acl.AclBinding;

/** An existing Kafka ACL that Gravitino cannot express, with the reason. */
public final class UnmappedAcl {

  /** Why an ACL was not imported. */
  public enum Reason {
    /** The Principal type is not {@code User}. */
    NON_USER_PRINCIPAL,
    /** The Principal is not under the managed principal prefix; the plugin never touches it. */
    OUTSIDE_PRINCIPAL_PREFIX,
    /** The ACL restricts client hosts; Gravitino grants apply to every host. */
    HOST_RESTRICTED,
    /** A group ACL other than READ ALLOW on the Principal's own PREFIXED group prefix. */
    GROUP_NOT_PRINCIPAL_PREFIX,
    /** A resource other than Topic or Group, e.g. Cluster or TransactionalId. */
    UNSUPPORTED_RESOURCE_TYPE,
    /** A PREFIXED topic pattern or the literal wildcard topic. */
    NON_LITERAL_TOPIC_PATTERN,
    /** The topic is not in the catalog's {@code default} schema. */
    TOPIC_NOT_IN_CATALOG,
    /** An operation other than READ, WRITE or DESCRIBE, e.g. ALL, ALTER or DELETE. */
    UNSUPPORTED_OPERATION,
    /** DESCRIBE without any READ or WRITE on the same Principal and topic. */
    DESCRIBE_ONLY,
    /** An ALLOW that a DENY on the same Principal, topic and operation already overrides. */
    SHADOWED_BY_DENY,
    /**
     * A DESCRIBE or group ACL of the right shape that the imported grants do not regenerate, e.g.
     * the group READ of a Principal without any effective consume grant.
     */
    NOT_IMPLIED_BY_GRANT
  }

  private final AclBinding acl;
  private final Reason reason;

  /**
   * Creates an unmapped ACL.
   *
   * @param acl the ACL
   * @param reason why it was not imported
   */
  public UnmappedAcl(AclBinding acl, Reason reason) {
    this.acl = acl;
    this.reason = reason;
  }

  /**
   * @return the ACL
   */
  public AclBinding acl() {
    return acl;
  }

  /**
   * @return why it was not imported
   */
  public Reason reason() {
    return reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UnmappedAcl)) {
      return false;
    }
    UnmappedAcl that = (UnmappedAcl) o;
    return acl.equals(that.acl) && reason == that.reason;
  }

  @Override
  public int hashCode() {
    return Objects.hash(acl, reason);
  }

  @Override
  public String toString() {
    return reason + ": " + acl;
  }
}

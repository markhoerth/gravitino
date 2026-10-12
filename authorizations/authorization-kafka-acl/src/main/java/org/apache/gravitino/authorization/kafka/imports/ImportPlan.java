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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.kafka.common.acl.AclBinding;

/** The result of planning an import of existing Kafka ACLs. */
public final class ImportPlan {

  private final List<MappedGrant> mapped;
  private final List<UnmappedAcl> unmapped;
  private final List<AclBinding> implied;
  private final List<AclBinding> additions;

  /**
   * Creates a plan.
   *
   * @param mapped ACLs that become Gravitino Topic grants
   * @param unmapped ACLs that Gravitino cannot express
   * @param implied ACLs that the plugin regenerates from the mapped grants
   * @param additions ACLs the plugin will create after the import that do not exist today
   */
  public ImportPlan(
      List<MappedGrant> mapped,
      List<UnmappedAcl> unmapped,
      List<AclBinding> implied,
      List<AclBinding> additions) {
    this.mapped = ImmutableList.copyOf(mapped);
    this.unmapped = ImmutableList.copyOf(unmapped);
    this.implied = ImmutableList.copyOf(implied);
    this.additions = ImmutableList.copyOf(additions);
  }

  /**
   * @return ACLs that become Gravitino Topic grants
   */
  public List<MappedGrant> mapped() {
    return mapped;
  }

  /**
   * @return ACLs that Gravitino cannot express, with the reason
   */
  public List<UnmappedAcl> unmapped() {
    return unmapped;
  }

  /**
   * @return ACLs that the plugin regenerates from the mapped grants
   */
  public List<AclBinding> implied() {
    return implied;
  }

  /**
   * @return ACLs the plugin will create on its next reconcile that do not exist today, e.g. the
   *     DESCRIBE DENY that accompanies an imported READ DENY
   */
  public List<AclBinding> additions() {
    return additions;
  }
}

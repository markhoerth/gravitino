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
package org.apache.gravitino.catalog.mongodb.converter;

/**
 * Controls how a Gravitino table schema is derived from a MongoDB collection.
 *
 * <p>Instances are built from catalog properties and are immutable once constructed.
 */
public final class MongoSchemaConfig {

  /** Where column definitions come from. */
  public enum InferenceMode {
    /**
     * Use the collection's {@code $jsonSchema} validator only. Collections without a validator are
     * reported as having no derivable schema.
     */
    VALIDATOR,
    /** Ignore any validator and always sample documents. */
    SAMPLE,
    /** Use the validator when present, sample otherwise. This is the default. */
    VALIDATOR_THEN_SAMPLE
  }

  /** What to do when sampling observes incompatible types for the same field path. */
  public enum ConflictPolicy {
    /** Widen the field to {@code string}. */
    WIDEN_TO_STRING,
    /** Emit an external type carrying the observed BSON type names. */
    EXTERNAL_TYPE,
    /** Fail the load with an explanatory error. */
    FAIL
  }

  public static final String INFERENCE_MODE = "mongodb.schema-inference-mode";
  public static final String SAMPLE_SIZE = "mongodb.schema-sample-size";
  public static final String CONFLICT_POLICY = "mongodb.schema-conflict-policy";
  public static final String DECIMAL_SCALE = "mongodb.decimal128-scale";
  public static final String MAX_NESTING_DEPTH = "mongodb.schema-max-nesting-depth";
  public static final String ALLOW_ADDITIONAL_PROPERTIES = "mongodb.allow-additional-properties";

  /** BSON Decimal128 carries 34 significant digits and no declared scale. */
  public static final int DECIMAL128_PRECISION = 34;

  private final InferenceMode inferenceMode;
  private final int sampleSize;
  private final ConflictPolicy conflictPolicy;
  private final int decimalScale;
  private final int maxNestingDepth;
  private final boolean allowAdditionalProperties;

  private MongoSchemaConfig(Builder builder) {
    this.inferenceMode = builder.inferenceMode;
    this.sampleSize = builder.sampleSize;
    this.conflictPolicy = builder.conflictPolicy;
    this.decimalScale = builder.decimalScale;
    this.maxNestingDepth = builder.maxNestingDepth;
    this.allowAdditionalProperties = builder.allowAdditionalProperties;
  }

  public InferenceMode inferenceMode() {
    return inferenceMode;
  }

  public int sampleSize() {
    return sampleSize;
  }

  public ConflictPolicy conflictPolicy() {
    return conflictPolicy;
  }

  public int decimalScale() {
    return decimalScale;
  }

  public int maxNestingDepth() {
    return maxNestingDepth;
  }

  public boolean allowAdditionalProperties() {
    return allowAdditionalProperties;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static MongoSchemaConfig defaults() {
    return builder().build();
  }

  /** Builder for {@link MongoSchemaConfig}. */
  public static final class Builder {
    private InferenceMode inferenceMode = InferenceMode.VALIDATOR_THEN_SAMPLE;
    private int sampleSize = 1000;
    private ConflictPolicy conflictPolicy = ConflictPolicy.WIDEN_TO_STRING;
    private int decimalScale = 8;
    private int maxNestingDepth = 8;
    private boolean allowAdditionalProperties = true;

    private Builder() {}

    public Builder inferenceMode(InferenceMode mode) {
      this.inferenceMode = mode;
      return this;
    }

    public Builder sampleSize(int size) {
      if (size <= 0) {
        throw new IllegalArgumentException(SAMPLE_SIZE + " must be positive, got " + size);
      }
      this.sampleSize = size;
      return this;
    }

    public Builder conflictPolicy(ConflictPolicy policy) {
      this.conflictPolicy = policy;
      return this;
    }

    public Builder decimalScale(int scale) {
      if (scale < 0 || scale > DECIMAL128_PRECISION) {
        throw new IllegalArgumentException(
            DECIMAL_SCALE + " must be between 0 and " + DECIMAL128_PRECISION + ", got " + scale);
      }
      this.decimalScale = scale;
      return this;
    }

    public Builder maxNestingDepth(int depth) {
      if (depth < 1) {
        throw new IllegalArgumentException(MAX_NESTING_DEPTH + " must be at least 1, got " + depth);
      }
      this.maxNestingDepth = depth;
      return this;
    }

    public Builder allowAdditionalProperties(boolean allow) {
      this.allowAdditionalProperties = allow;
      return this;
    }

    public MongoSchemaConfig build() {
      return new MongoSchemaConfig(this);
    }
  }
}

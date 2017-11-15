/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.decimal;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.serde.ComplexMetrics;

import java.util.List;

public class DecimalDruidModule implements DruidModule
{

  public static final String DECIMALSUM = "decimalSum";
  public static final String DECIMALMAX = "decimalMax";
  public static final String DECIMALMIN = "decimalMin";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("DecimalModule").registerSubtypes(
            new NamedType(DecimalSumAggregatorFactory.class, DECIMALSUM)
        ),
        new SimpleModule("DecimalModule").registerSubtypes(
            new NamedType(DecimalMaxAggregatorFactory.class, DECIMALMAX)
        ),
        new SimpleModule("DecimalModule").registerSubtypes(
            new NamedType(DecimalMinAggregatorFactory.class, DECIMALMIN)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (ComplexMetrics.getSerdeForType(DECIMALSUM) == null) {
      ComplexMetrics.registerSerde(DECIMALSUM, new DecimalSumSerde());
    }

    if (ComplexMetrics.getSerdeForType(DECIMALMIN) == null) {
      ComplexMetrics.registerSerde(DECIMALMIN, new DecimalMinSerde());
    }

    if (ComplexMetrics.getSerdeForType(DECIMALMAX) == null) {
      ComplexMetrics.registerSerde(DECIMALMAX, new DecimalMaxSerde());
    }

  }
}

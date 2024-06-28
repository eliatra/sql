/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CharMatcher;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.SerializableBiFunction;

/**
 * The definition of json functions. 1) have the clear interface for function define. 2) the
 * implementation should rely on ExprValue.
 */
@UtilityClass
public class JsonFunction {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    Configuration.setDefaults(
        new Configuration.Defaults() {

          private final JsonProvider jsonProvider = new JacksonJsonProvider();
          private final MappingProvider mappingProvider = new JacksonMappingProvider();

          @Override
          public JsonProvider jsonProvider() {
            return jsonProvider;
          }

          @Override
          public MappingProvider mappingProvider() {
            return mappingProvider;
          }

          @Override
          public Set<Option> options() {
            return Set.of(Option.SUPPRESS_EXCEPTIONS);
          }
        });
  }

  /**
   * Register Json Functions.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(extract());
    // repository.register(keys());
  }

  private DefaultFunctionResolver extract() {
    return define(
        BuiltinFunctionName.JSON_EXTRACT.getName(),
        impl(
            nullMissingHandling(
                (SerializableBiFunction<ExprValue, ExprValue, ExprValue>)
                    (jsonExpr, pathExpr) -> {
                      String jsonInput = jsonExpr.value().toString();
                      String path = CharMatcher.is('\"').trimFrom(pathExpr.toString());

                      try {

                        // when path start with $ we interpret it as json path
                        // everything else is interpreted as json pointer
                        if (path.startsWith("$")) {
                          Object retVal = JsonPath.parse(jsonInput).read(path);

                          if (retVal instanceof String) {
                            return ExprValueUtils.fromObjectValue(retVal);
                          }

                          String retVal2 = MAPPER.writeValueAsString(retVal);
                          return ExprValueUtils.fromObjectValue(retVal2);
                        } else {
                          JsonNode retVal = MAPPER.readTree(jsonInput).at(path);
                          if (retVal.isValueNode()) {
                            return ExprValueUtils.fromObjectValue(retVal.asText());
                          }

                          return ExprValueUtils.fromObjectValue(retVal.toString());
                        }
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    }),
            STRING,
            STRING,
            STRING));
  }

  private DefaultFunctionResolver keys() {
    return null;
  }
}

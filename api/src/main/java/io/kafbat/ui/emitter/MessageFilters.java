package io.kafbat.ui.emitter;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.Strings.CS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.NullValue;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelOptions;
import dev.cel.common.CelValidationException;
import dev.cel.common.CelValidationResult;
import dev.cel.common.types.CelType;
import dev.cel.common.types.CelTypeProvider;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.extensions.CelExtensions;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.kafbat.ui.exception.CelException;
import io.kafbat.ui.model.TopicMessageDTO;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;

@Slf4j
@UtilityClass
public class MessageFilters {

  private static final String CEL_RECORD_VAR_NAME = "record";
  private static final String CEL_RECORD_TYPE_NAME = TopicMessageDTO.class.getSimpleName();

  // CEL record fields
  private static final String PARTITION = "partition";
  private static final String OFFSET = "offset";
  private static final String TIMESTAMP_MS = "timestampMs";
  private static final String KEY = "key";
  private static final String KEY_AS_TEXT = "keyAsText";
  private static final String VALUE = "value";
  private static final String VALUE_AS_TEXT = "valueAsText";
  private static final String HEADERS = "headers";

  private static final CelCompiler CEL_COMPILER = createCompiler();
  private static final CelRuntime CEL_RUNTIME = createRuntime();
  private static final Object CELL_NULL_VALUE = NullValue.NULL_VALUE;

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
  private static final Pattern HEX_IN_UNICODE_ESCAPE = Pattern.compile("(?<=\\\\u)[0-9A-F]{4}");

  public static Predicate<TopicMessageDTO> noop() {
    return e -> true;
  }

  public static Predicate<TopicMessageDTO> containsStringFilter(String string) {
    @Nullable String escapedUpper = escapeNonAscii(string);
    @Nullable String escapedLower = escapedUpper != null
        ? HEX_IN_UNICODE_ESCAPE.matcher(escapedUpper).replaceAll(m -> m.group().toLowerCase())
        : null;
    return msg -> msgContains(msg, string)
        || (escapedUpper != null && msgContains(msg, escapedUpper))
        || (escapedLower != null && msgContains(msg, escapedLower));
  }

  @Nullable
  static String escapeNonAscii(String input) {
    String escaped = StringEscapeUtils.escapeJson(input);
    return escaped.equals(input) ? null : escaped;
  }

  private static boolean msgContains(TopicMessageDTO msg, String search) {
    return CS.contains(msg.getKey(), search)
        || CS.contains(msg.getValue(), search)
        || headersContains(msg, search);
  }

  private static boolean headersContains(TopicMessageDTO msg, String searchString) {
    final var headers = msg.getHeaders();

    if (headers == null) {
      return false;
    }

    for (final var entry : headers.entrySet()) {
      if (CS.contains(entry.getKey(), searchString) || CS.contains(entry.getValue(), searchString)) {
        return true;
      }
    }

    return false;
  }

  public static Predicate<TopicMessageDTO> celScriptFilter(String script) {
    CelValidationResult celValidationResult = CEL_COMPILER.compile(script);
    if (celValidationResult.hasError()) {
      throw new CelException(script, celValidationResult.getErrorString());
    }

    try {
      CelAbstractSyntaxTree ast = celValidationResult.getAst();
      CelRuntime.Program program = CEL_RUNTIME.createProgram(ast);

      return createPredicate(script, program);
    } catch (CelValidationException | CelEvaluationException e) {
      throw new CelException(script, e);
    }
  }

  private static Predicate<TopicMessageDTO> createPredicate(String originalScript, CelRuntime.Program program) {
    return topicMessage -> {
      Object programResult;
      try {
        programResult = program.eval(Map.of(CEL_RECORD_VAR_NAME, new LazyRecordArgs(topicMessage)));
      } catch (CelEvaluationException e) {
        throw new CelException(originalScript, e);
      }

      if (programResult instanceof Boolean isMessageMatched) {
        return isMessageMatched;
      }

      throw new CelException(
          originalScript,
          "Unexpected script result, boolean should be returned instead. Script output: %s".formatted(programResult)
      );
    };
  }

  // Record fields for CEL script. Fields are computed on access, so json parsing of key/value
  // is only done when script uses record.key / record.value (e.g. not for record.keyAsText == "...")
  @SuppressWarnings("java:S2160") // used only as CEL variable, never compared
  private static final class LazyRecordArgs extends AbstractMap<String, Object> {

    private static final List<String> FIELDS =
        List.of(PARTITION, OFFSET, TIMESTAMP_MS, KEY, KEY_AS_TEXT, VALUE, VALUE_AS_TEXT, HEADERS);

    private final TopicMessageDTO msg;
    private final Map<String, Object> parsed = new HashMap<>();

    LazyRecordArgs(TopicMessageDTO msg) {
      this.msg = msg;
    }

    @Override
    @Nullable
    public Object get(Object field) {
      if (!(field instanceof String name)) {
        return null;
      }
      return switch (name) {
        case PARTITION -> msg.getPartition();
        case OFFSET -> msg.getOffset();
        case TIMESTAMP_MS -> msg.getTimestamp() != null ? msg.getTimestamp().toInstant().toEpochMilli() : null;
        case KEY -> msg.getKey() != null
            ? parsed.computeIfAbsent(name, n -> parseToJsonOrReturnAsIs(msg.getKey()))
            : null;
        case KEY_AS_TEXT -> msg.getKey();
        case VALUE -> msg.getValue() != null
            ? parsed.computeIfAbsent(name, n -> parseToJsonOrReturnAsIs(msg.getValue()))
            : null;
        case VALUE_AS_TEXT -> msg.getValue();
        case HEADERS -> Objects.requireNonNullElse(msg.getHeaders(), emptyMap());
        default -> null;
      };
    }

    @Override
    public boolean containsKey(Object field) {
      // not parsing key/value json just to check presence
      if (KEY.equals(field)) {
        return msg.getKey() != null;
      }
      if (VALUE.equals(field)) {
        return msg.getValue() != null;
      }
      return get(field) != null;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
      Map<String, Object> all = new LinkedHashMap<>();
      FIELDS.forEach(f -> {
        Object v = get(f);
        if (v != null) {
          all.put(f, v);
        }
      });
      return all.entrySet();
    }

    @Nullable
    private static Object parseToJsonOrReturnAsIs(@Nullable String str) {
      if (str == null) {
        return null;
      }

      try {
        //@formatter:off
        var map = OBJECT_MAPPER.readValue(str, new TypeReference<Map<String, Object>>() {});
        //@formatter:on
        return replaceCelNulls(map);
      } catch (JsonProcessingException e) {
        return str;
      }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> replaceCelNulls(Map<String, Object> map) {
      var result = new LinkedHashMap<String, Object>();

      for (var entry : map.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        if (value == null) {
          result.put(key, CELL_NULL_VALUE);
        } else if (value instanceof Map<?, ?>) {
          var inner = (Map<String, Object>) value;
          result.put(key, replaceCelNulls(inner));
        } else {
          result.put(key, value);
        }
      }

      return result;
    }
  }

  private static CelCompiler createCompiler() {
    Map<String, CelType> fields = Map.of(
        PARTITION, SimpleType.INT,
        OFFSET, SimpleType.INT,
        TIMESTAMP_MS, SimpleType.INT,
        KEY_AS_TEXT, SimpleType.STRING,
        VALUE_AS_TEXT, SimpleType.STRING,
        HEADERS, MapType.create(SimpleType.STRING, SimpleType.STRING),
        KEY, SimpleType.DYN,
        VALUE, SimpleType.DYN
    );

    ImmutableSet<String> names = ImmutableSet
        .<String>builder()
        .addAll(fields.keySet())
        .build();

    StructType recordType = StructType.create(
        CEL_RECORD_TYPE_NAME,
        names,
        fieldName -> Optional.ofNullable(fields.get(fieldName))
    );

    return CelCompilerFactory.standardCelCompilerBuilder()
        .setOptions(CelOptions.DEFAULT)
        .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
        .addLibraries(CelExtensions.strings(), CelExtensions.encoders())
        .addVar(CEL_RECORD_VAR_NAME, recordType)
        .setResultType(SimpleType.BOOL)
        .setTypeProvider(new CelTypeProvider() {
          @Override
          public ImmutableCollection<CelType> types() {
            return ImmutableSet.of(recordType);
          }

          @Override
          public Optional<CelType> findType(String typeName) {
            return CEL_RECORD_TYPE_NAME.equals(typeName) ? Optional.of(recordType) : Optional.empty();
          }
        })
        .build();
  }

  private static CelRuntime createRuntime() {
    return CelRuntimeFactory.standardCelRuntimeBuilder()
        .addLibraries(CelExtensions.strings(), CelExtensions.encoders())
        .build();
  }

}

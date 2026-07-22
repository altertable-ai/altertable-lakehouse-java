package ai.altertable.lakehouse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;

/** A typed, synchronous client for the Altertable Lakehouse HTTP API. */
public final class LakehouseClient {
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(60);
  private final HttpClient http;
  private final ObjectMapper json;
  private final URI baseUrl;
  private final String authorization;
  private final Duration requestTimeout;
  private final int retries;
  private final String userAgent;

  public LakehouseClient(Config config) {
    Objects.requireNonNull(config, "config");
    this.baseUrl = config.baseUrl();
    this.authorization = config.authorization();
    this.requestTimeout = config.requestTimeout();
    this.retries = config.retries();
    this.userAgent = config.userAgent();
    this.http = config.httpClient() == null
        ? HttpClient.newBuilder().connectTimeout(config.connectTimeout()).version(HttpClient.Version.HTTP_2).build()
        : config.httpClient();
    this.json = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public AppendResponse append(String catalog, String schema, String table, AppendRequest request, Boolean sync) {
    requireText(catalog, "catalog"); requireText(schema, "schema"); requireText(table, "table");
    Objects.requireNonNull(request, "request");
    Map<String, String> parameters = new LinkedHashMap<>();
    parameters.put("catalog", catalog); parameters.put("schema", schema); parameters.put("table", table);
    if (sync != null) parameters.put("sync", sync.toString());
    return sendJson("append", "POST", "/append", parameters, request, AppendResponse.class);
  }

  public TaskResponse getTask(UUID taskId) {
    return sendJson("getTask", "GET", "/tasks/" + requireId(taskId, "taskId"), Map.of(), null, TaskResponse.class);
  }

  /** Executes a query and exposes its NDJSON rows lazily. Close the result when iteration ends early. */
  public QueryResult query(QueryRequest request) {
    Objects.requireNonNull(request, "request"); requireText(request.statement(), "statement");
    HttpResponse<InputStream> response = executeStream(streamRequest("POST", "/query", Map.of(), request), "query");
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      try (InputStream body = response.body()) { throw failure("query", "POST", "/query", response, body.readAllBytes(), null); }
      catch (IOException error) { throw new NetworkError("query", "POST", "/query", null, true, null, error); }
    }
    try { return new QueryResult(response.body(), json); }
    catch (IOException error) { try { response.body().close(); } catch (IOException ignored) { } throw new ParseError("query", "POST", "/query", null, false, null, error); }
  }

  public QueryAllResult queryAll(QueryRequest request) {
    try (QueryResult result = query(request)) {
      List<List<JsonNode>> rows = new ArrayList<>();
      result.forEach(rows::add);
      return new QueryAllResult(result.metadata(), result.columns(), List.copyOf(rows));
    }
  }

  public QueryLogResponse getQuery(UUID queryId) {
    return sendJson("getQuery", "GET", "/query/" + requireId(queryId, "queryId"), Map.of(), null, QueryLogResponse.class);
  }

  public CancelQueryResponse cancelQuery(UUID queryId, String sessionId) {
    return sendJson("cancelQuery", "DELETE", "/query/" + requireId(queryId, "queryId"), Map.of("session_id", requireText(sessionId, "sessionId")), null, CancelQueryResponse.class);
  }

  public void upload(String catalog, String schema, String table, UploadMode mode, byte[] contents, String contentType) {
    fileRequest("upload", "/upload", Map.of("catalog", requireText(catalog, "catalog"), "schema", requireText(schema, "schema"),
        "table", requireText(table, "table"), "mode", Objects.requireNonNull(mode, "mode").value()), contents, contentType);
  }

  public void upsert(String catalog, String schema, String table, String primaryKey, byte[] contents, String contentType) {
    fileRequest("upsert", "/upsert", Map.of("catalog", requireText(catalog, "catalog"), "schema", requireText(schema, "schema"),
        "table", requireText(table, "table"), "primary_key", requireText(primaryKey, "primaryKey")), contents, contentType);
  }

  public ValidateResponse validate(ValidateRequest request) {
    Objects.requireNonNull(request, "request"); requireText(request.statement(), "statement");
    return sendJson("validate", "POST", "/validate", Map.of(), request, ValidateResponse.class);
  }

  public AutocompleteResponse autocomplete(AutocompleteRequest request) {
    Objects.requireNonNull(request, "request"); requireText(request.statement(), "statement");
    return sendJson("autocomplete", "POST", "/autocomplete", Map.of(), request, AutocompleteResponse.class);
  }

  private void fileRequest(String operation, String path, Map<String, String> parameters, byte[] contents, String contentType) {
    Objects.requireNonNull(contents, "contents");
    HttpRequest.Builder request = request("POST", path, parameters).POST(HttpRequest.BodyPublishers.ofByteArray(contents));
    request.header("Content-Type", contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType);
    HttpResponse<byte[]> response = execute(request.build(), operation);
    ensureSuccess(operation, "POST", path, response);
  }

  private <T> T sendJson(String operation, String method, String path, Map<String, String> parameters, Object body, Class<T> type) {
    HttpRequest.Builder request = request(method, path, parameters).header("Accept", "application/json");
    if (body == null) request.method(method, HttpRequest.BodyPublishers.noBody());
    else {
      try { request.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(body))); }
      catch (IOException error) { throw new SerializationError(operation, method, path, null, false, null, error); }
    }
    HttpResponse<byte[]> response = execute(request.build(), operation);
    ensureSuccess(operation, method, path, response);
    try { return json.readValue(response.body(), type); }
    catch (IOException error) { throw new ParseError(operation, method, path, response.statusCode(), false, requestId(response), error); }
  }

  private HttpRequest.Builder request(String method, String path, Map<String, String> parameters) {
    return HttpRequest.newBuilder(uri(path, parameters)).timeout(requestTimeout).header("Authorization", authorization)
        .header("User-Agent", userAgent).header("Accept", "application/json");
  }

  private HttpRequest streamRequest(String method, String path, Map<String, String> parameters, Object body) {
    try {
      return request(method, path, parameters).header("Content-Type", "application/json")
          .header("Accept", "application/x-ndjson").method(method, HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(body))).build();
    } catch (IOException error) { throw new SerializationError("query", method, path, null, false, null, error); }
  }

  private URI uri(String path, Map<String, String> parameters) {
    StringJoiner query = new StringJoiner("&");
    parameters.forEach((key, value) -> { if (value != null) query.add(encode(key) + "=" + encode(value)); });
    String separator = query.length() == 0 ? "" : "?" + query;
    return baseUrl.resolve(path + separator);
  }

  private static String encode(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20"); }

  private HttpResponse<byte[]> execute(HttpRequest request, String operation) {
    int attempt = 0;
    while (true) {
      try {
        HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() < 500 || attempt >= retries) return response;
      } catch (java.net.http.HttpTimeoutException error) {
        if (attempt >= retries) throw new TimeoutError(operation, request.method(), request.uri().getPath(), null, true, null, error);
      } catch (IOException error) {
        if (attempt >= retries) throw new NetworkError(operation, request.method(), request.uri().getPath(), null, true, null, error);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new NetworkError(operation, request.method(), request.uri().getPath(), null, false, null, error);
      }
      attempt++;
    }
  }

  private HttpResponse<InputStream> executeStream(HttpRequest request, String operation) {
    try { return http.send(request, HttpResponse.BodyHandlers.ofInputStream()); }
    catch (java.net.http.HttpTimeoutException error) { throw new TimeoutError(operation, request.method(), request.uri().getPath(), null, true, null, error); }
    catch (IOException error) { throw new NetworkError(operation, request.method(), request.uri().getPath(), null, true, null, error); }
    catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new NetworkError(operation, request.method(), request.uri().getPath(), null, false, null, error); }
  }

  private void ensureSuccess(String operation, String method, String path, HttpResponse<byte[]> response) {
    if (response.statusCode() < 200 || response.statusCode() >= 300) throw failure(operation, method, path, response, response.body(), null);
  }

  private LakehouseException failure(String operation, String method, String path, HttpResponse<?> response, byte[] body, Throwable cause) {
    int status = response.statusCode(); String requestId = requestId(response);
    String detail = "HTTP status " + status;
    if (status == 401 || status == 403) return new AuthError(operation, method, path, status, false, requestId, detail, cause);
    if (status == 400 || status == 404 || status == 422) return new BadRequestError(operation, method, path, status, false, requestId, detail, cause);
    return new ApiError(operation, method, path, status, status >= 500, requestId, detail, cause);
  }

  private static String requestId(HttpResponse<?> response) { return response.headers().firstValue("x-request-id").orElse(response.headers().firstValue("x-correlation-id").orElse(null)); }
  private static String requireText(String value, String name) { if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank"); return value; }
  private static UUID requireId(UUID value, String name) { return Objects.requireNonNull(value, name); }

  public static final class Config {
    private URI baseUrl = URI.create("https://api.altertable.ai");
    private String username; private String password; private String basicToken;
    private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT; private Duration requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private int retries = 2; private String userAgentSuffix; private HttpClient httpClient;
    public Config baseUrl(String value) { this.baseUrl = URI.create(requireText(value, "baseUrl")); return this; }
    public Config credentials(String value, String secret) { this.username = requireText(value, "username"); this.password = requireText(secret, "password"); return this; }
    /** Sets either a base64 Basic token or a full {@code Basic <token>} header value. */
    public Config basicToken(String value) { this.basicToken = requireText(value, "basicToken"); return this; }
    public Config connectTimeout(Duration value) { this.connectTimeout = Objects.requireNonNull(value, "connectTimeout"); return this; }
    public Config requestTimeout(Duration value) { this.requestTimeout = Objects.requireNonNull(value, "requestTimeout"); return this; }
    public Config retries(int value) { if (value < 0) throw new IllegalArgumentException("retries must be non-negative"); this.retries = value; return this; }
    public Config userAgentSuffix(String value) { this.userAgentSuffix = value; return this; }
    public Config httpClient(HttpClient value) { this.httpClient = value; return this; }
    private URI baseUrl() { return baseUrl.toString().endsWith("/") ? URI.create(baseUrl.toString().substring(0, baseUrl.toString().length() - 1)) : baseUrl; }
    private Duration connectTimeout() { return connectTimeout; } private Duration requestTimeout() { return requestTimeout; } private int retries() { return retries; } private HttpClient httpClient() { return httpClient; }
    private String userAgent() { return "altertable-lakehouse-java/0.1.0" + (userAgentSuffix == null || userAgentSuffix.isBlank() ? "" : " " + userAgentSuffix); }
    private String authorization() {
      String token = basicToken;
      if (token == null) token = System.getenv("ALTERTABLE_BASIC_AUTH_TOKEN");
      if (token != null && !token.isBlank()) return token.startsWith("Basic ") ? token : "Basic " + token;
      String resolvedUser = username == null ? System.getenv("ALTERTABLE_LAKEHOUSE_USERNAME") : username;
      String resolvedPassword = password == null ? System.getenv("ALTERTABLE_LAKEHOUSE_PASSWORD") : password;
      if (resolvedUser == null || resolvedPassword == null) throw new ConfigurationError("client", null, null, null, false, null, null);
      return "Basic " + Base64.getEncoder().encodeToString((resolvedUser + ":" + resolvedPassword).getBytes(StandardCharsets.UTF_8));
    }
  }

  public enum ComputeSize { XS, S, M, L, XL, AUTO }
  public enum UploadMode { CREATE("create"), APPEND("append"), OVERWRITE("overwrite"); private final String value; UploadMode(String value) { this.value = value; } @JsonValue public String value() { return value; } }
  public enum AppendErrorCode { INVALID_DATA("invalid-data"), INCOMPATIBLE_SCHEMA("incompatible-schema"); private final String value; AppendErrorCode(String value) { this.value = value; } @JsonValue public String value() { return value; } @JsonCreator public static AppendErrorCode from(String value) { for (AppendErrorCode code : values()) if (code.value.equals(value)) return code; throw new IllegalArgumentException("Unknown append error code: " + value); } }
  public enum TaskStatus { PENDING("pending"), COMPLETED("completed"); private final String value; TaskStatus(String value) { this.value = value; } @JsonValue public String value() { return value; } @JsonCreator public static TaskStatus from(String value) { return valueOf(value.toUpperCase(Locale.ROOT)); } }
  public enum SessionKind { ArrowFlightSQL, HttpQuery, HttpCancel, HttpValidate, HttpExplain, HttpAutocomplete, Postgres }

  /** A JSON object or JSON array, preserving the append endpoint's one-of request contract. */
  public record AppendRequest(JsonNode payload) {
    public AppendRequest { if (payload == null || (!payload.isObject() && !payload.isArray())) throw new IllegalArgumentException("AppendRequest must be an object or array"); }
    @JsonValue public JsonNode json() { return payload; }
    public static AppendRequest single(Map<String, ?> value) { return new AppendRequest(new ObjectMapper().valueToTree(value)); }
    public static AppendRequest batch(List<? extends Map<String, ?>> values) { return new AppendRequest(new ObjectMapper().valueToTree(values)); }
  }
  public record AppendResponse(@JsonProperty("ok") boolean ok, @JsonProperty("error_code") AppendErrorCode errorCode, @JsonProperty("error_message") String errorMessage, @JsonProperty("task_id") UUID taskId) { }
  public record TaskResponse(@JsonProperty("task_id") UUID taskId, @JsonProperty("status") TaskStatus status) { }
  public record QueryRequest(String statement, String catalog, String schema, @JsonProperty("session_id") String sessionId, @JsonProperty("compute_size") ComputeSize computeSize, Boolean sanitize, Long limit, Long offset, String timezone, Boolean ephemeral, Boolean visible, @JsonProperty("requested_by") String requestedBy, @JsonProperty("query_id") String queryId, Boolean cache, String dialect) { public static QueryRequest of(String statement) { return new QueryRequest(statement, null, null, null, null, null, null, null, null, null, null, null, null, null); } }
  public record ValidateRequest(String statement, String catalog, String schema, @JsonProperty("session_id") String sessionId) { public static ValidateRequest of(String statement) { return new ValidateRequest(statement, null, null, null); } }
  public record ValidateResponse(boolean valid, String statement, @JsonProperty("connections_errors") Map<String, String> connectionsErrors, String error) { }
  public record AutocompleteRequest(String statement, String catalog, String schema, @JsonProperty("session_id") String sessionId, @JsonProperty("max_suggestions") Integer maxSuggestions) { public static AutocompleteRequest of(String statement) { return new AutocompleteRequest(statement, null, null, null, null); } }
  public record AutocompleteSuggestion(String suggestion, @JsonProperty("suggestion_start") int suggestionStart, @JsonProperty("suggestion_type") String suggestionType, @JsonProperty("suggestion_score") long suggestionScore, @JsonProperty("extra_char") String extraChar) { }
  public record AutocompleteResponse(List<AutocompleteSuggestion> suggestions, String statement, @JsonProperty("connections_errors") Map<String, String> connectionsErrors) { }
  public record CancelQueryResponse(boolean cancelled, String message) { }
  public record CachingStats(@JsonProperty("data_hits") long dataHits, @JsonProperty("data_misses") long dataMisses, @JsonProperty("data_bytes_hits") long dataBytesHits, @JsonProperty("data_bytes_misses") long dataBytesMisses, @JsonProperty("filehandle_hits") long filehandleHits, @JsonProperty("filehandle_misses") long filehandleMisses, @JsonProperty("metadata_hits") long metadataHits, @JsonProperty("metadata_misses") long metadataMisses) { }
  public record MemoryStats(@JsonProperty("total_usage_bytes") long totalUsageBytes) { }
  public record ScanStats(@JsonProperty("estimated_result_rows") long estimatedResultRows, @JsonProperty("estimated_scanned_rows") long estimatedScannedRows) { }
  public record QueryStats(CachingStats caching, MemoryStats memory, ScanStats scan) { }
  public record QueryProgress(double percentage, @JsonProperty("rows_processed") long rowsProcessed, @JsonProperty("total_rows") long totalRows) { }
  public record QueryLogResponse(UUID uuid, @JsonProperty("start_time") String startTime, @JsonProperty("end_time") String endTime, @JsonProperty("duration_ms") Long durationMs, String query, @JsonProperty("session_id") String sessionId, @JsonProperty("client_interface") SessionKind clientInterface, String error, QueryStats stats, QueryProgress progress, Boolean visible, @JsonProperty("requested_by") String requestedBy, @JsonProperty("user_agent") String userAgent) { }
  public record QueryAllResult(JsonNode metadata, List<String> columns, List<List<JsonNode>> rows) { }

  /** A single-use, lazy iterator over an NDJSON response. */
  public static final class QueryResult implements Iterable<List<JsonNode>>, AutoCloseable {
    private final BufferedReader reader; private final ObjectMapper json; private final JsonNode metadata; private final List<String> columns; private boolean iterated;
    private QueryResult(InputStream body, ObjectMapper json) throws IOException {
      this.reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8)); this.json = json;
      this.metadata = parse(reader.readLine(), 1); JsonNode schema = parse(reader.readLine(), 2);
      if (!schema.isArray()) throw new ParseError("query", "POST", "/query", null, false, null, new IOException("NDJSON schema line is not an array"));
      List<String> names = new ArrayList<>(); for (JsonNode column : schema) names.add(column.asText()); this.columns = List.copyOf(names);
    }
    public JsonNode metadata() { return metadata; } public List<String> columns() { return columns; }
    @Override public Iterator<List<JsonNode>> iterator() {
      if (iterated) throw new IllegalStateException("QueryResult rows can only be iterated once"); iterated = true;
      return new Iterator<>() { private String next = nextLine(); private int lineIndex = 3;
        @Override public boolean hasNext() { return next != null; }
        @Override public List<JsonNode> next() { if (next == null) throw new NoSuchElementException(); String line = next; next = nextLine(); JsonNode row = parse(line, lineIndex++); if (!row.isArray()) throw new ParseError("query", "POST", "/query", null, false, null, new IOException("NDJSON row is not an array")); List<JsonNode> values = new ArrayList<>(); row.forEach(values::add); return List.copyOf(values); }
      };
    }
    private String nextLine() { try { return reader.readLine(); } catch (IOException error) { throw new ParseError("query", "POST", "/query", null, true, null, error); } }
    private JsonNode parse(String line, int index) { if (line == null) throw new ParseError("query", "POST", "/query", null, false, null, new IOException("Unexpected end of NDJSON at line " + index)); try { return json.readTree(line); } catch (IOException error) { throw new ParseError("query", "POST", "/query", null, false, null, new IOException("Malformed NDJSON at line " + index + ": " + line, error)); } }
    @Override public void close() { try { reader.close(); } catch (IOException ignored) { } }
  }

  public static class LakehouseException extends RuntimeException {
    private final String operation; private final String method; private final String path; private final Integer statusCode; private final boolean retriable; private final String requestId;
    LakehouseException(String operation, String method, String path, Integer statusCode, boolean retriable, String requestId, String message, Throwable cause) { super(message == null ? operation + " failed" : operation + " failed: " + message, cause); this.operation = operation; this.method = method; this.path = path; this.statusCode = statusCode; this.retriable = retriable; this.requestId = requestId; }
    public String operation() { return operation; } public String method() { return method; } public String path() { return path; } public Integer statusCode() { return statusCode; } public boolean retriable() { return retriable; } public String requestId() { return requestId; }
  }
  public static final class AuthError extends LakehouseException { AuthError(String o, String m, String p, Integer s, boolean r, String id, String message, Throwable c) { super(o, m, p, s, r, id, message, c); } }
  public static final class BadRequestError extends LakehouseException { BadRequestError(String o, String m, String p, Integer s, boolean r, String id, String message, Throwable c) { super(o, m, p, s, r, id, message, c); } }
  public static final class NetworkError extends LakehouseException { NetworkError(String o, String m, String p, Integer s, boolean r, String id, Throwable c) { super(o, m, p, s, r, id, null, c); } }
  public static final class TimeoutError extends LakehouseException { TimeoutError(String o, String m, String p, Integer s, boolean r, String id, Throwable c) { super(o, m, p, s, r, id, null, c); } }
  public static final class SerializationError extends LakehouseException { SerializationError(String o, String m, String p, Integer s, boolean r, String id, Throwable c) { super(o, m, p, s, r, id, null, c); } }
  public static final class ParseError extends LakehouseException { ParseError(String o, String m, String p, Integer s, boolean r, String id, Throwable c) { super(o, m, p, s, r, id, null, c); } }
  public static final class ApiError extends LakehouseException { ApiError(String o, String m, String p, Integer s, boolean r, String id, String message, Throwable c) { super(o, m, p, s, r, id, message, c); } }
  public static final class ConfigurationError extends LakehouseException { ConfigurationError(String o, String m, String p, Integer s, boolean r, String id, Throwable c) { super(o, m, p, s, r, id, "missing Lakehouse credentials", c); } }
}

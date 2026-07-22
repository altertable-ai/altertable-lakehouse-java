# Altertable Lakehouse Java SDK

[![CI](https://github.com/altertable-ai/altertable-lakehouse-java/actions/workflows/ci.yml/badge.svg)](https://github.com/altertable-ai/altertable-lakehouse-java/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/ai.altertable/altertable-lakehouse-java.svg)](https://central.sonatype.com/artifact/ai.altertable/altertable-lakehouse-java)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A typed Java 17 client for querying and ingesting data through the Altertable Lakehouse API.

## Install

```xml
<dependency>
  <groupId>ai.altertable</groupId>
  <artifactId>altertable-lakehouse-java</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Quick start

```java
import ai.altertable.lakehouse.LakehouseClient;

var client = new LakehouseClient(new LakehouseClient.Config()
    .credentials(System.getenv("ALTERTABLE_LAKEHOUSE_USERNAME"),
        System.getenv("ALTERTABLE_LAKEHOUSE_PASSWORD")));
var result = client.queryAll(LakehouseClient.QueryRequest.of("SELECT 1"));
System.out.println(result.rows());
```

## API reference

### Initialization

Create `LakehouseClient` with `LakehouseClient.Config`. The client uses Java's built-in pooled `HttpClient`, with a 5-second connection timeout, a 60-second request timeout, and two retries for network and server failures.

### Querying

- `query(QueryRequest)` returns a lazy `QueryResult`. Read `metadata()`, `columns()`, and iterate rows; call `close()` if you stop early.
- `queryAll(QueryRequest)` returns `QueryAllResult` with all rows collected.
- `getQuery(UUID)` returns `QueryLogResponse`.
- `cancelQuery(UUID, String)` cancels a query for the supplied session.

```java
var stream = client.query(LakehouseClient.QueryRequest.of("SELECT * FROM events"));
for (var row : stream) System.out.println(row);
stream.close();

var log = client.getQuery(queryId);
var cancelled = client.cancelQuery(queryId, sessionId);
```

### Ingestion

- `append(String, String, String, AppendRequest, Boolean)` appends one JSON object or an array of objects.
- `getTask(UUID)` polls an asynchronous append task.
- `upload(String, String, String, UploadMode, byte[], String)` uploads CSV, JSON, or Parquet data.
- `upsert(String, String, String, String, byte[], String)` upserts file data using a primary-key column.

```java
var append = client.append("catalog", "public", "events",
    LakehouseClient.AppendRequest.single(java.util.Map.of("event", "signed_up")), true);
var task = append.taskId() == null ? null : client.getTask(append.taskId());
client.upload("catalog", "public", "events", LakehouseClient.UploadMode.CREATE,
    csvBytes, "text/csv");
client.upsert("catalog", "public", "events", "id", csvBytes, "text/csv");
```

### SQL helpers

- `validate(ValidateRequest)` validates a SQL statement.
- `autocomplete(AutocompleteRequest)` returns SQL completion suggestions.

```java
var valid = client.validate(LakehouseClient.ValidateRequest.of("SELECT 1"));
var suggestions = client.autocomplete(LakehouseClient.AutocompleteRequest.of("SEL"));
```

### Errors

All SDK failures extend `LakehouseException` and expose operation, method, path, status code, retryability, and request ID. The typed subclasses are `AuthError`, `BadRequestError`, `NetworkError`, `TimeoutError`, `SerializationError`, `ParseError`, `ApiError`, and `ConfigurationError`.

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `baseUrl` | `String` | `https://api.altertable.ai` | API origin. |
| `credentials` | `String, String` | environment | Basic-auth username and password. |
| `basicToken` | `String` | environment | Pre-encoded Basic token, with or without the `Basic ` prefix. |
| `connectTimeout` | `Duration` | 5 seconds | TCP connection timeout. |
| `requestTimeout` | `Duration` | 60 seconds | Per-request response timeout. |
| `retries` | `int` | 2 | Retries after network errors or 5xx responses. |
| `userAgentSuffix` | `String` | none | Appended to the SDK User-Agent. |
| `httpClient` | `HttpClient` | pooled Java client | Overrides the SDK-managed HTTP client. |

When direct credentials are not configured, the client reads `ALTERTABLE_LAKEHOUSE_USERNAME` and `ALTERTABLE_LAKEHOUSE_PASSWORD`, or `ALTERTABLE_BASIC_AUTH_TOKEN`. It fails at construction with `ConfigurationError` when none are available.

## Development

Use Java 17 and Maven.

```bash
mvn dependency:resolve
mvn test
mvn verify
```

The integration suite runs against `ghcr.io/altertable-ai/altertable-mock:latest`. Locally, Testcontainers starts the mock automatically; CI provides it as a service on port `15000`. No production credentials are required.

Release Please creates version and changelog PRs from Conventional Commit titles. After a release PR merges, GitHub Actions publishes the signed JAR, sources JAR, and Javadoc JAR to Maven Central using the repository's protected release secrets.

## License

This project is licensed under the [MIT License](LICENSE).

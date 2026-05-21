# Repository Guidelines

## Project Structure & Module Organization

This is a Gradle multi-module Java project.

- `utility/`: shared utilities, including JSON/YAML helpers.
- `core/`: Lucene storage, S3/cache directory logic, metadata providers, cluster state, routing, search, and indexing services.
- `server/`: CLI entry point, Vert.x HTTP API, request routing, distributed query/write coordination, and runtime resources such as `logback.xml`.
- Tests live under each module's `src/test/java`.
- Runtime data, WAL, cache, logs, and generated distributions are build/runtime artifacts and should not be committed.

## Build, Test, and Development Commands

Use JDK 25. On Windows:

```powershell
$env:JAVA_HOME='C:\Users\wxk6b\Documents\Helper\jdk-25.0.2\'
$env:Path="$env:JAVA_HOME\bin;$env:Path"
```

Key commands:

- `.\gradlew.bat test --stacktrace`: run all tests.
- `.\gradlew.bat :core:test --stacktrace`: run core tests only.
- `.\gradlew.bat :server:test --stacktrace`: run server/API tests only.
- `.\gradlew.bat :server:run --args="server --http-port 9200 --data-path data/node-1"`: start a local node.
- `.\gradlew.bat :server:distZip`: build `server/build/distributions/server-1.0-SNAPSHOT.zip`.

For etcd integration tests, set `ETCD_TEST_ENDPOINTS=http://127.0.0.1:2379`.

## Coding Style & Naming Conventions

Use Java 25, 4-space indentation, and clear package ownership under `com.github.wxk6b1203`. Prefer existing patterns before adding abstractions. Keep comments short and only where they clarify non-obvious behavior. Public read preferences are `weak` and `strong`; `owner` and `remote` are internal implementation values.

## Testing Guidelines

Tests use JUnit Jupiter. Name tests after behavior, for example `nonMasterWriteReroutesStaleShardOwnerThroughMaster`. Add focused tests for routing, metadata transitions, Lucene directory behavior, and HTTP APIs when changing those areas. Multi-node etcd tests should be gated with `@EnabledIfEnvironmentVariable`.

## Commit & Pull Request Guidelines

Commit messages follow:

```text
<type>(<scope>): <subject>
```

Examples: `fix(http): reroute stale shard owner writes`, `test(server): cover remote shard rpc paths`. Keep subjects concise and under 200 characters.

Pull requests should describe the behavior change, list verification commands, note etcd/S3 assumptions, and call out any migration or configuration impact.

## Architecture Notes

Do not reintroduce primary/replica semantics. S3 is the durable source for committed Lucene files, while each shard has one current write owner. Cluster state and manifest metadata are etcd-backed in multi-node mode.

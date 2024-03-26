Rocker
========

Rocker is NIF for Erlang which uses Rust binding for [RocksDB](https://github.com/facebook/rocksdb). Its key features are safety, performance and a minimal codebase. The keys and data are kept binary and this doesn’t impose any restrictions on storage format. So far the project is suitable for being used in third-party solutions.

## Build Information
* "rocker" uses the [rebar3](https://www.rebar3.org) build system. Makefile so that simply running "make" at the top level should work.
* "rocker" requires Erlang >= 20.1.
* "rocker" requires Rust >= 1.23.
* "rocker" requires Clang >= 3.8.

## Rebar3 deps
```
{deps, [
   {rocker, "", {git, "git://github.com/Vonmo/rocker.git", {tag, "v8.10.0"}}},
]}.
```

## mix deps (Elixir)
```
defp deps do
 [
   {:rocker, git: "https://github.com/Vonmo/rocker.git", tag: "v8.10.0"}
 ]
end
```

## Features
* kv operations
* column families support
* batch write
* support of flexible storage setup
* range iterator
* delete range
* multi get
* snapshots
* checkpoints (Online backups)
* backup api

## Main requirements for a driver
* Reliability
* Performance
* Minimal codebase
* Safety
* Functionality


## Performance
In a set of tests you can find a performance test. It demonstrates about 135k write RPS and 2.1M read RPS on my machine. In real conditions we might expect something about 50k write RPS and 400k read RPS with average amount of data being about 1 kB per key and the total number of keys exceeding 1 billion.

## Status
Passed all the functional and performance tests.

## Versioning
The release version follows the RocksDB's one.
For instance, if the version of rocker is 5.14.2 then the version of RocksDB rocker binds is also 5.14.2.

## License
rocker's license is [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

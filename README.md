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
   {rocker, "", {git, "git://github.com/Vonmo/rocker.git", {tag, "v5.14.2_2"}}},
]}.
```

## mix deps (Elixir)
```
defp deps do
 [
   {:rocker, git: "https://github.com/Vonmo/rocker.git", tag: "v5.14.2_2"}      
 ]
end
```

## Features
* all basics db operations
* column families support
* transactions support

## Main requirements for a driver
* Reliability
* Performance
* Minimal codebase
* Safety
* Functionality
* All the main kv-functions
* Column families
* Transactions
* Data compression
* Support of flexible storage setup

## API
### Open database
You can work with database in two modes:

1. Default column family. In this mode all your keys are stored in the same set. Rocksdb makes it possible to fine-tune the storage flexibly according to your current tasks. Depending on them, database can be opened in two different ways:
using a standard set of options
```
{ok, Db} = rocker:open_default(<<"/project/priv/db_default_path">>).
```
This operation will result in a pointer for working with database. The database itself will be blocked for any other attempts to open it. Immediately after clearing of this pointer it will be unlocked automatically.
fine-tuning options to your needs
```
{ok, Db} = rocker:open(<<"/project/priv/db_path">>, #{
create_if_missing => true,
set_max_open_files => 1000,
set_use_fsync => false,
...
set_disable_auto_compactions => true,
set_compaction_style => universal
}).
```
2. Split into several column families. Keys are saved into the so-called column families, and every family might have various options. Let’s take a look at an example of database opening with standard options for all column families:
```
{ok, Db} = case rocker:list_cf(BookDbPath) of
{ok, CfList} ->
   rocker:open_cf_default(BookDbPath, CfList);
_ ->
   CfList = [],
   rocker:open_default(BookDbPath)
end.
```
### Delete database
To delete database `correctly rocker:destroy(Path)` should be run while the database shouldn’t be used.

#### Recover database after a failure
In case of a system failure database can be recovered with the help of `rocker:repair(Path)`. The process consists of 4 steps:
1. file search
1. recovering tables by WAL replaying
1. metadata extraction
1. writing of descriptor

#### Column family creation
`rocker:create_cf_default(Db, <<"testcf1">>) -> ok.`

#### Column family deletion
`rocker:drop_cf(Db, <<"testcf">>) -> ok.`
 
### CRUD operations
#### Data writing by key
`rocker:put(Db, <<"key">>, <<"value">>) -> ok.`

#### Data acquisition by key
`rocker:get(Db, <<"key">>) -> {ok, <<"value">>} | notfound`

#### Data deletion by key
`rocker:delete(Db, <<"key">>) -> ok.`

#### Data writing by key within CF
`rocker:put_cf(Db, <<"testcf">>, <<"key">>, <<"value">>) -> ok.`

#### Data acquisition by key within CF
`rocker:get_cf(Db, <<"testcf">>, <<"key">>) -> {ok, <<"value">>} | notfound`

#### Data deletion by key within CF
`rocker:delete_cf(Db, <<"testcf">>, <<"key">>) -> ok.`

### Iterators
As you know, one of the basic principles of rocksdb is organized key storage. This feature is vital in real tasks and, to use it properly, we need data iterators. In rocksdb there are a few iteration modes. You can find the code samples in the tests: https://github.com/Vonmo/rocker/blob/master/test/rocker_SUITE.erl
* From table beginning. In Rocker the `{'start'}` iterator is responsible for that.
* From table end: `{'end'}`
* From a certain key forward `{'from', Key, forward}`
* From a certain key reverse `{'from', Key, reverse}`
It should be noted that all these modes also work for iteration mode in column families.

#### Create iterator
`rocker:iterator(Db, {'start'}) -> {ok, Iter}.`

#### Check iterator
`rocker:iterator_valid(Iter) -> {ok, true} | {ok, false}.`

#### Create iterator for CF
`rocker:iterator_cf(Db, Cf, {'start'}) -> {ok, Iter}.`

#### Create prefix iterator
While creating a database, any prefix iterator requires a clear indication of prefix length.
```
{ok, Db} = rocker:open(Path, #{
prefix_length => 3
}).
```
Here is an example of creating an iterator by prefix ‘aaa’
```
{ok, Iter} = rocker:prefix_iterator(Db, <<"aaa">>),
```

#### Create prefix iterator for CF
Like the previous prefix iterator, this one needs the indication of prefix_length for column family.
`{ok, Iter} = rocker:prefix_iterator_cf(Db, Cf, <<"aaa">>),`

#### Get the following element
`rocker:next(Iter) -> {ok, <<"key">>, <<"value">>} | ok`
This method returns the following key/value or ok if the iterator was completed.

### Transactions
It’s commonly required to simultaneously write the changes of a key set. Rocker allows us to unite CRUD operations both within a common set and in CF. The following example illustrates our work with transactions:
```
{ok, 6} = rocker:tx(Db, [
   {put, <<"k1">>, <<"v1">>},
   {put, <<"k2">>, <<"v2">>},
   {delete, <<"k0">>, <<"v0">>},
   {put_cf, Cf, <<"k1">>, <<"v1">>},
   {put_cf, Cf, <<"k2">>, <<"v2">>},
   {delete_cf, Cf, <<"k0">>, <<"v0">>}
]).
```

## Performance
In a set of tests you can find a performance test. It demonstrates about 30k read RPS and 200k write RPS on my machine. In real conditions we might expect something about 15–20k read RPS and 120k write RPS with average amount of data being about 1 kB per key and the total number of keys exceeding 1 billion.

## Status
Passed all the functional and performance tests.

## Versioning
The release version follows the RocksDB's one.
For instance, if the version of rocker is 5.14.2 then the version of RocksDB rocker binds is also 5.14.2.

## License
rocker's license is [Apache License Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)

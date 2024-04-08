### Installing the extension
You need to first install pgrx to compile this extension. You can easily do that by running the below command on your computer.
> $ cargo install --locked cargo-pgrx

Follow this [instruction](https://github.com/pgcentralfoundation/pgrx/blob/develop/cargo-pgrx/README.md#first-time-initialization) to connect your pgrx to your postgres installation. You can always submit an issue in this repo if you have a problem with the connection.

Open the Postgres conf file (`postgresql.conf`) and add `postgres_redis` to the [shared_preload_libraries](https://pgpedia.info/s/shared_preload_libraries.html) parameter.

In the same conf file, set the following parameters:
* `postgres_redis.redis_url (string)`: Redis service URL.
* `postgres_redis.table (string)`: Table to monitor for select and update query execution.
* `postgres_redis.key_column(string)`: Table column name whose value will be used as the redis key when running the `set` command.
* `postgres_redis.value_column(string)`: Table column name whose value will be used as the redis value when running the `set` command.

This is an example usage:

```
# postgresql.conf
shared_preload_libraries = 'postgres_redis'

postgres_redis.redis_url = 'redis://127.0.0.1'
postgres_redis.table = 'users'
postgres_redis.key_column = 'first_name'
postgres_redis.value_column = 'last_name'
```

### Running the extension
Once you can successfully run `cargo pgrx status all` in your system, just cd into this folder and run `cargo pgrx run` in your favorite terminal to compile this extension. Add the extension to your postgres by running `CREATE EXTENSION postgres_redis;` and  then have fun!

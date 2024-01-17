### Running the extension
You need to first install pgrx to compile this extension. You can easily do that by running the below command on your computer.
> $ cargo install --locked cargo-pgrx

Follow this [instruction](https://github.com/pgcentralfoundation/pgrx/blob/develop/cargo-pgrx/README.md#first-time-initialization) to connect your pgrx to your postgres installation. You can always submit an issue in this repo if you have a problem with the connection.

Once you can successfully run `cargo pgrx status all` in your system, just cd into this folder and run `cargo pgrx run` in your favorite terminal to compile this extension. Add the extension to your postgres by running `CREATE EXTENSION postgres_redis;`, then have fun!

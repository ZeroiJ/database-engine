# Installing rustdb globally

```bash
cargo install --path .
```

This will install three binaries:
- `rustdb` - The main REPL
- `rustdb-server` - TCP server mode
- `rustdb-client` - CLI client for connecting to the server

## Usage

```bash
# Start the REPL
rustdb mydb.json

# Start a server
rustdb-server mydb.json 7878

# Connect to a server
rustdb-client localhost 7878
```

## Development

Use the `just` command runner for common tasks:

```bash
just install    # Install binaries
just run        # Run REPL
just server     # Run server
just client     # Run client
just test       # Run tests
just bench      # Run benchmark
```

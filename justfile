# Install all binaries to ~/.cargo/bin
install:
    cargo install --path .

# Run REPL
run db="rustdb.json":
    cargo run --bin rustdb -- {{db}}

# Run server
server db="rustdb.json" port="7878":
    cargo run --bin rustdb-server -- {{db}} {{port}}

# Run client
client host="localhost" port="7878":
    cargo run --bin rustdb-client -- {{host}} {{port}}

# Run tests
test:
    cargo test

# Run bench
bench:
    cargo run --bin rustdb -- rustdb.json

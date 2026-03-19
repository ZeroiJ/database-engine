fn main() {
    let args: Vec<String> = std::env::args().collect();
    let db_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "rustdb.json".to_string());
    let port: u16 = args.get(2).and_then(|p| p.parse().ok()).unwrap_or(7878);

    database_engine::server::start(db_path, port);
}

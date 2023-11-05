use std::{
    env,
    fs::File,
    process::{Command, Stdio},
};

pub fn insert_data(generated_sql_path: String) -> Result<(), Box<dyn std::error::Error>> {
    let generated_sql_file = File::open(generated_sql_path)?;
    let memcached_url = env::var("MEMCACHED_URL")?;
    let disable_memcached_flush: bool = env::var("DISABLE_MEMCACHED_FLUSH")?
        .parse()
        .unwrap_or(false);

    let mut child = Command::new("mysql")
        .arg(format!("-u{}", env::var("MYSQL_USER").unwrap()))
        .arg(format!("-p{}", env::var("MYSQL_PASSWORD").unwrap()))
        .arg(format!("-S{}", env::var("MYSQL_SOCKET").unwrap()))
        .arg("--default-character-set=utf8mb4")
        .arg(env::var("MYSQL_DATABASE").unwrap())
        .stdin(Stdio::from(generated_sql_file))
        .spawn()?;
    let exit_status = child.wait()?;
    println!("{}", exit_status);

    if !disable_memcached_flush {
        let cache_client = memcache::connect(memcached_url)?;
        cache_client.flush()?;
    }

    Ok(())
}

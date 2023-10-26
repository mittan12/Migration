use std::{
    env::{self, VarError},
    fs::File,
    process::{Command, Stdio},
};

pub fn insert_data(generated_sql_path: String) -> Result<(), Box<dyn std::error::Error>> {
    let generated_sql_file = File::open(generated_sql_path)?;

    let memcached_url = match env::var("MEMCACHED_URL") {
        Ok(s) => s,
        Err(VarError::NotPresent) => panic!("$MEMCACHED_URL is not set."),
        Err(VarError::NotUnicode(_)) => panic!("$MEMCACHED_URL should be written in Unicode."),
    };

    Command::new("mysql")
        .arg(format!("-u{}", env::var("MYSQL_USER").unwrap()))
        .arg(format!("-p{}", env::var("MYSQL_PASSWORD").unwrap()))
        .arg(format!("-S{}", env::var("MYSQL_SOCKET").unwrap()))
        .arg("--default-character-set=utf8mb4")
        .arg(env::var("MYSQL_DATABASE").unwrap())
        .stdin(Stdio::from(generated_sql_file))
        .spawn()
        .expect("failed to import generated sql file");

    let cache_client = memcache::connect(memcached_url).unwrap();
    cache_client.flush().unwrap();

    Ok(())
}

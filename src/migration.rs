use std::{
    env,
    fs::File,
    process::{Command, Stdio},
};

pub fn insert_data(generated_sql_path: String) -> Result<(), Box<dyn std::error::Error>> {
    let generated_sql_file = File::open(generated_sql_path)?;

    Command::new("mysql")
        .arg(format!("-u{}", env::var("MYSQL_USER").unwrap()))
        .arg(format!("-p{}", env::var("MYSQL_PASSWORD").unwrap()))
        // .arg(format!("-S{}", config.socket))
        .arg(env::var("MYSQL_DATABASE").unwrap())
        .stdin(Stdio::from(generated_sql_file))
        .spawn()
        .expect("failed to import generated sql file");

    Ok(())
}

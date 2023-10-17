use migration::{generator::generate_sql, migration::insert_data};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::from_filename(".env.local").ok();

    let generated_sql_path = generate_sql()?;
    insert_data(generated_sql_path)?;

    Ok(())
}

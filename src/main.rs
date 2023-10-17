use migration::generator::generate_sql;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    generate_sql()
}

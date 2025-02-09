use std::{
    env::{self, VarError},
    fs,
    path::Path,
};

use csv::{ReaderBuilder, StringRecord};

pub fn generate_sql() -> Result<String, Box<dyn std::error::Error>> {
    let out_path = match env::var("SQL_OUT_PATH") {
        Ok(s) => s,
        Err(env::VarError::NotPresent) => "out.sql".to_string(),
        Err(VarError::NotUnicode(_)) => panic!("$SQL_OUT_PATH should be written in Unicode."),
    };

    let data_path = Path::new("data");

    let entries = fs::read_dir(data_path)?;
    let mut file_list: Vec<_> = entries
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.is_file() && path.extension()? == "csv" {
                Some(path.file_name()?.to_string_lossy().into_owned())
            } else {
                None
            }
        })
        .collect();
    file_list.sort();

    let mut sql_lines = Vec::new();

    for file_name in &file_list {
        if file_name
            .split('!')
            .nth(1)
            .unwrap_or_default()
            .split('.')
            .nth(1)
            .unwrap_or_default()
            != "csv"
        {
            continue;
        }

        let mut rdr = ReaderBuilder::new().from_path(data_path.join(file_name))?;
        let headers_record = rdr.headers()?;
        let headers: Vec<String> = headers_record
            .into_iter()
            .map(|row| row.to_string())
            .collect();

        let mut csv_data: Vec<StringRecord> = Vec::new();
        let mut rdr = ReaderBuilder::new().from_path(data_path.join(file_name))?;
        let records: Vec<StringRecord> = rdr.records().filter_map(|row| row.ok()).collect();
        csv_data.extend(records);

        let table_name = file_name
            .split('!')
            .nth(1)
            .unwrap_or_default()
            .split('.')
            .next()
            .unwrap_or_default();

        let mut sql_lines_inner = Vec::new();
        sql_lines_inner.push(format!(
            "LOCK TABLES `{}` WRITE;\nINSERT INTO `{}` VALUES ",
            table_name, table_name
        ));

        for (idx, data) in csv_data.iter().enumerate() {
            let cols: Vec<_> = data
                .iter()
                .enumerate()
                .filter_map(|(col_idx, col)| {
                    if headers
                        .get(col_idx)
                        .unwrap_or(&String::new())
                        .starts_with('#')
                    {
                        return None;
                    }

                    if col.is_empty() {
                        Some("NULL".to_string())
                    } else {
                        Some(format!("'{}'", col.replace('\'', "\\'")))
                    }
                })
                .collect();

            sql_lines_inner.push(if idx == csv_data.len() - 1 {
                format!("({});", cols.join(","))
            } else {
                format!("({}),", cols.join(","))
            });
        }

        sql_lines.push(format!("{}\nUNLOCK TABLES", sql_lines_inner.concat()));
    }

    let create_sql: String =
        String::from_utf8_lossy(&fs::read(data_path.join("create_table.sql"))?).parse()?;

    fs::write(
        out_path.clone(),
        format!("{}{};", create_sql, sql_lines.join(";")),
    )?;

    Ok(out_path)
}

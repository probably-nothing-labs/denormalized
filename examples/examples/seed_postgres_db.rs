use tokio_postgres::{Error, GenericClient, NoTls};

/// docker run --name postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=postgres_db -p 5432:5432 -d postgres:16-alpine
#[tokio::main]
async fn main() -> Result<(), Error> {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=postgres password=password dbname=postgres_db",
        NoTls,
    )
    .await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let table_name = "companies";
    client
        .execute(
            format!(
                r#"CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, name VARCHAR)"#,
                table_name
            )
            .as_str(),
            &[],
        )
        .await?;

    let stmt = client
        .prepare(format!("INSERT INTO {} (name) VALUES ($1)", table_name).as_str())
        .await?;
    client.execute(&stmt, &[&"test"]).await?;

    Ok(())
}

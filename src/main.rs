use clickhouse::sql::Identifier;
use clickhouse::{Client, Row};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
// use rdkafka::message::Headers;
use serde::{Serialize, Deserialize};

#[derive(Row, Serialize, Deserialize)]
struct MyRow {
    user_id: String,
    event: String,
}

#[derive(Serialize, Deserialize)]
struct MyMessage {
}

async fn consume_to_clickhouse(client: &Client, brokers: &str, group_id: &str, topics: &[&str]) -> Result<(), std::io::Error> {
    let mut inserter = client.inserter("test")
        .unwrap()
        .with_max_rows(100);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation error");

    consumer.subscribe(topics)
            .expect("Cannot subscribe to topics");

    while let Ok(m) = consumer.recv().await {
        let payload = match m.payload_view::<str>() {
            None => "",
            Some(Ok(s)) => s,
            Some(Err(e)) => {
                println!("Error deserializing payload {:?}", e);
                ""
            }

        };
        println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
            m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
        let row: MyRow = serde_json::from_str(payload).expect("Failed to Deserialize payload");
        inserter.write(&row)?;//.unwrap();
        inserter.commit().await?;//.unwrap();

        consumer.commit_message(&m, CommitMode::Async).unwrap();

    }

    inserter.end().await?;
    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let client = Client::default()
        .with_url("http://localhost:8123");
        // .with_database("test");

    client
        .query(
            "CREATE OR REPLACE TABLE ? (
                user_id String,
                event String,

            )
            ENGINE = MergeTree
            ORDER BY user_id",
        )
        .bind(Identifier("test"))
        .execute()
        .await?;


    consume_to_clickhouse(&client, "localhost:9094", "my_group", &["quickstart-events"]).await?;

    Ok(())
}


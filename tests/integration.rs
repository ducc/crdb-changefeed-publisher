use assert_cmd::cargo::cargo_bin;
use aws_sdk_sqs::{Credentials, Region};
use aws_smithy_http::endpoint::Endpoint;
use dockertest::waitfor::{MessageSource, MessageWait};
use dockertest::{
    Composition, DockerOperations, DockerTest, Image, PullPolicy, Source, StartPolicy,
};
use http::Uri;
use portpicker::pick_unused_port;
use rexpect::session::spawn_command;
use sqlx::{PgPool, Pool, Postgres};
use std::process::Command;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;
use test_log::test;
use tracing::{debug, info};
use uuid::Uuid;
use warp::http;

const COCKROACH_HANDLE: &str = "cockroach";

fn create_cockroach_comp() -> Composition {
    let cockroach_image = Image::with_repository("cockroachdb/cockroach")
        .tag("v22.1.8")
        .pull_policy(PullPolicy::IfNotPresent);

    let mut comp = Composition::with_image(cockroach_image)
        .with_container_name(COCKROACH_HANDLE)
        .with_wait_for(Box::new(MessageWait {
            source: MessageSource::Stdout,
            timeout: 30,
            message: "end running init files".into(),
        }))
        .with_start_policy(StartPolicy::Strict)
        .with_cmd(vec![
            "start-single-node".to_string(),
            "--insecure".to_string(),
        ]);

    comp.publish_all_ports();

    comp
}

const SQS_HANDLE: &str = "sqs";
fn create_sqs_comp() -> Composition {
    let sqs_image = Image::with_repository("roribio16/alpine-sqs")
        .tag("1.2.0")
        .pull_policy(PullPolicy::IfNotPresent);

    let mut comp = Composition::with_image(sqs_image)
        .with_container_name(SQS_HANDLE)
        .with_start_policy(StartPolicy::Strict)
        .with_wait_for(Box::new(MessageWait {
            source: MessageSource::Stdout,
            timeout: 60,
            message: "Started SQS rest server".into(),
        }));

    comp.publish_all_ports();

    comp
}

fn create_test_enviroment() -> DockerTest {
    let mut test = DockerTest::new().with_default_source(Source::DockerHub);

    test.add_composition(create_cockroach_comp());
    test.add_composition(create_sqs_comp());

    test
}

async fn get_sqlx_pool(ops: &DockerOperations) -> (String, Pool<Postgres>) {
    let cockroach_handle = ops.handle(COCKROACH_HANDLE);
    let host_port = cockroach_handle.host_port_unchecked(26257);
    let db_url = format!(
        "postgresql://root@{}:{}/defaultdb?sslmode=disable",
        host_port.0, host_port.1
    );

    (
        db_url.to_string(),
        PgPool::connect(db_url.as_str()).await.unwrap(),
    )
}

async fn get_sqs_sdk(ops: &DockerOperations) -> (String, aws_sdk_sqs::Client) {
    let sqs_handle = ops.handle(SQS_HANDLE);
    let host_port = sqs_handle.host_port_unchecked(9324);
    let sqs_url = format!("http://{}:{}", host_port.0, host_port.1);

    let config = aws_config::from_env()
        .endpoint_resolver(Endpoint::immutable(
            Uri::from_str(sqs_url.as_str()).unwrap(),
        ))
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "test"));
    let config = config.load().await;

    (sqs_url, aws_sdk_sqs::Client::new(&config))
}

async fn create_test_queue(sdk: &aws_sdk_sqs::Client) -> String {
    let create_queue_response = sdk
        .create_queue()
        .queue_name("test-queue")
        .send()
        .await
        .unwrap();

    create_queue_response.queue_url.unwrap()
}

fn streamer_cmd(
    db_url: &str,
    queue_url: &str,
    changefeed_id: &str,
    table_name: &str,
    sqs_url: &str,
) -> Command {
    let exec = cargo_bin("crdb-changefeed-publisher")
        .into_os_string()
        .into_string()
        .unwrap();
    // let cmd = format!("{} --queue sqs --table {}", exec, table_name);
    let mut cmd = Command::new(exec);
    let prom_addr = format!("0.0.0.0:{}", pick_unused_port().unwrap());

    cmd.env("DATABASE_URL", db_url)
        .env("SQS_QUEUE_URL", queue_url)
        .env("CHANGEFEED_ID", changefeed_id)
        .env("PROMETHEUS_ADDR", prom_addr)
        .env("AWS_REGION", "us-east-1")
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_SQS_ENDPOINT", sqs_url)
        .arg("--queue")
        .arg("sqs")
        .arg("--table")
        .arg(table_name);

    cmd
}

struct TestValue {
    key: String,
    value: String,
}

impl Default for TestValue {
    fn default() -> Self {
        Self {
            key: Uuid::new_v4().to_string(),
            value: Uuid::new_v4().to_string(),
        }
    }
}

#[test]
fn basic_test() {
    let test = create_test_enviroment();

    test.run(|ops| async move {
        let (db_url, sql_pool) = get_sqlx_pool(&ops).await;

        sqlx::query("CREATE TABLE IF NOT EXISTS test_table (key STRING NOT NULL PRIMARY KEY, value STRING NOT NULL);")
            .execute(&sql_pool)
            .await
            .unwrap();

        sqlx::query("SET cluster setting kv.rangefeed.enabled=true;")
            .execute(&sql_pool)
            .await
            .unwrap();

        let (sqs_url, sdk) = get_sqs_sdk(&ops).await;
        let queue_url = create_test_queue(&sdk).await;

        info!("Queue URL: {}", queue_url);

        let cmd = streamer_cmd(&db_url, &queue_url, "test", "test_table", sqs_url.as_str());
        let mut shell = spawn_command(cmd, Some(10000)).unwrap();

        shell.exp_string("Starting changefeed against node:").unwrap();

        for _ in 0..10 {
            let test_value = TestValue::default();
            sqlx::query("INSERT INTO test_table (key, value) VALUES ($1, $2)")
                .bind(&test_value.key)
                .bind(&test_value.value)
                .execute(&sql_pool)
                .await
                .unwrap();
        }

        let receive_message_response = sdk
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(15)
            .send()
            .await
            .unwrap();

        let mut messages = receive_message_response.messages.unwrap();

        assert_eq!(messages.len(), 1);
        let content = messages.pop().unwrap().body.unwrap();
        let parsed_content: serde_json::Value = serde_json::from_str(content.as_str()).unwrap();
        let content_set = parsed_content.as_array().unwrap();

        debug!("Parsed content: {:?}", parsed_content);
        assert_eq!(content_set.len(), 10);

        sqlx::query("SELECT * from defaultdb.public.cursor_store WHERE key = $1")
            .bind("test")
            .fetch_one(&sql_pool)
            .await
            .unwrap();
    })
}

#[test]
fn publish_missed_messages() {
    let test = create_test_enviroment();

    test.run(|ops| async move {
        let (db_url, sql_pool) = get_sqlx_pool(&ops).await;

        sqlx::query("CREATE TABLE IF NOT EXISTS test_table (key STRING NOT NULL PRIMARY KEY, value STRING NOT NULL);")
            .execute(&sql_pool)
            .await
            .unwrap();

        sqlx::query("SET cluster setting kv.rangefeed.enabled=true;")
            .execute(&sql_pool)
            .await
            .unwrap();

        let (sqs_url, sdk) = get_sqs_sdk(&ops).await;
        let queue_url = create_test_queue(&sdk).await;

        info!("Queue URL: {}", queue_url);


        // Start the streamer
        let cmd = streamer_cmd(&db_url, &queue_url, "test", "test_table", sqs_url.as_str());
        let mut shell = spawn_command(cmd, Some(10000)).unwrap();

        shell.exp_string("Starting changefeed against node:").unwrap();
        shell.exp_string("UPSERT INTO cursor_store").unwrap();

        // Kill the streamer after an insert has been made to the cursor table
        shell.send_control('c').unwrap();
        shell.exp_eof().unwrap();

        for _ in 0..10 {
            let test_value = TestValue::default();
            sqlx::query("INSERT INTO test_table (key, value) VALUES ($1, $2)")
                .bind(&test_value.key)
                .bind(&test_value.value)
                .execute(&sql_pool)
                .await
                .unwrap();
        }

        let cmd = streamer_cmd(&db_url, &queue_url, "test", "test_table", sqs_url.as_str());
        let mut shell = spawn_command(cmd, Some(10000)).unwrap();

        shell.exp_string("Starting changefeed against node:").unwrap();

        let receive_message_response = sdk
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(15)
            .send()
            .await
            .unwrap();

        let mut messages = receive_message_response.messages.unwrap();

        assert_eq!(messages.len(), 1);
        let content = messages.pop().unwrap().body.unwrap();
        let parsed_content: serde_json::Value = serde_json::from_str(content.as_str()).unwrap();
        let content_set = parsed_content.as_array().unwrap();

        debug!("Parsed content: {:?}", parsed_content);
        assert_eq!(content_set.len(), 10);
    })
}

#[test]
fn recover_from_bad_cursor() {
    let test = create_test_enviroment();

    test.run(|ops| async move {
        let (db_url, sql_pool) = get_sqlx_pool(&ops).await;

        sqlx::query("CREATE TABLE IF NOT EXISTS test_table (key STRING NOT NULL PRIMARY KEY, value STRING NOT NULL);")
            .execute(&sql_pool)
            .await
            .unwrap();

        sqlx::query("SET cluster setting kv.rangefeed.enabled=true;")
            .execute(&sql_pool)
            .await
            .unwrap();

        sqlx::query("alter range default configure zone using gc.ttlseconds = 1;")
            .execute(&sql_pool)
            .await
            .unwrap();

        sleep(Duration::from_secs(1));

        let (sqs_url, sdk) = get_sqs_sdk(&ops).await;
        let queue_url = create_test_queue(&sdk).await;

        info!("Queue URL: {}", queue_url);


        // Start the streamer
        let cmd = streamer_cmd(&db_url, &queue_url, "test", "test_table", sqs_url.as_str());
        let mut shell = spawn_command(cmd, Some(10000)).unwrap();

        shell.exp_string("Starting changefeed against node:").unwrap();
        shell.exp_string("UPSERT INTO cursor_store").unwrap();

        // Kill the streamer after an insert has been made to the cursor table
        shell.send_control('c').unwrap();
        shell.exp_eof().unwrap();

        for _ in 0..10 {
            let test_value = TestValue::default();
            sqlx::query("INSERT INTO test_table (key, value) VALUES ($1, $2)")
                .bind(&test_value.key)
                .bind(&test_value.value)
                .execute(&sql_pool)
                .await
                .unwrap();
        }

        sqlx::query("update defaultdb.public.cursor_store set cursor = 1 where key = 'test';")
            .execute(&sql_pool)
            .await
            .unwrap();

        let cmd = streamer_cmd(&db_url, &queue_url, "test", "test_table", sqs_url.as_str());
        let mut shell = spawn_command(cmd, Some(10000)).unwrap();

        shell.exp_string("Starting changefeed against node:").unwrap();

        while let Ok(line) = shell.read_line() {
            info!("Line: {}", line);
        }

        let receive_message_response = sdk
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(15)
            .send()
            .await
            .unwrap();

        let mut messages = receive_message_response.messages.unwrap();

        assert_eq!(messages.len(), 1);
        let content = messages.pop().unwrap().body.unwrap();
        let parsed_content: serde_json::Value = serde_json::from_str(content.as_str()).unwrap();
        let content_set = parsed_content.as_array().unwrap();

        debug!("Parsed content: {:?}", parsed_content);
        assert_eq!(content_set.len(), 10);
    })
}

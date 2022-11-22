use dockertest::waitfor::{MessageSource, MessageWait};
use dockertest::{
    Composition, DockerOperations, DockerTest, Image, PullPolicy, Source, StartPolicy,
};
use sqlx::{PgPool, Pool, Postgres};
use test_log::test;

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
            timeout: 30,
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

async fn get_sqlx_pool(ops: &DockerOperations) -> Pool<Postgres> {
    let cockroach_handle = ops.handle(COCKROACH_HANDLE);
    let host_port = cockroach_handle.host_port_unchecked(26257);
    let db_url = format!(
        "postgresql://root@localhost:{}/defaultdb?sslmode=disable",
        host_port.1
    );

    PgPool::connect(db_url.as_str()).await.unwrap()
}

#[test]
fn startup_test() {
    let test = create_test_enviroment();

    test.run(|ops| async move {
        let sql_pool = get_sqlx_pool(&ops).await;

        sqlx::query("CREATE TABLE IF NOT EXISTS test_table (key STRING NOT NULL PRIMARY KEY, value STRING NOT NULL);")
            .execute(&sql_pool)
            .await
            .unwrap();
    })
}

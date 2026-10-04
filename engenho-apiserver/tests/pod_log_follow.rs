use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use engenho_apiserver::{
    ApiError, ApiServer, LogQuery, PodLogReader, ResourceHandler, StoreBackedHandler,
};
use engenho_store::{InProcessRouter, StoreMesh, default_config};

#[derive(Default)]
struct GrowingLog {
    text: Mutex<String>,
    gone: Mutex<bool>,
}

#[async_trait]
impl PodLogReader for GrowingLog {
    async fn read_pod_logs(
        &self,
        _ns: &str,
        _name: &str,
        query: &LogQuery,
    ) -> Result<String, ApiError> {
        if *self.gone.lock().unwrap() {
            return Err(ApiError::NotFound("pod is gone".into()));
        }
        let text = self.text.lock().unwrap().clone();
        let from = usize::try_from(query.from_byte.unwrap_or(0))
            .unwrap()
            .min(text.len());
        let rest = &text[from..];
        Ok(match query.tail_lines {
            Some(n) => engenho_apiserver::pod_logs::last_lines(rest, n),
            None => rest.to_owned(),
        })
    }
}

async fn serve(log: Arc<GrowingLog>) -> ApiServer {
    let router = InProcessRouter::new();
    let store = Arc::new(
        StoreMesh::start(
            1,
            "in-process://1".into(),
            router,
            default_config("pod-log-follow").unwrap(),
        )
        .await
        .unwrap(),
    );
    store.initialize_singleton().await.unwrap();
    assert!(store.wait_for_leadership(Duration::from_secs(3)).await);
    let descriptor = engenho_types::generated_v1_34::RESOURCE_CATALOG
        .iter()
        .find(|d| d.group.is_empty() && d.kind == "Pod")
        .expect("Pod is cataloged");
    let pods: Arc<dyn ResourceHandler> =
        Arc::new(StoreBackedHandler::from_descriptor(store, descriptor).with_log_reader(log));
    ApiServer::start("127.0.0.1:0".parse().unwrap(), vec![pods], None)
        .await
        .unwrap()
}

async fn next_chunk(resp: &mut reqwest::Response) -> Option<String> {
    tokio::time::timeout(Duration::from_secs(3), resp.chunk())
        .await
        .expect("a follow chunk arrived in time")
        .expect("chunk read")
        .map(|b| String::from_utf8(b.to_vec()).unwrap())
}

#[tokio::test]
async fn a_followed_log_streams_what_is_appended_and_ends_when_the_log_goes_away() {
    let log = Arc::new(GrowingLog::default());
    *log.text.lock().unwrap() = "one\ntwo\n".into();
    let server = serve(log.clone()).await;
    let url = format!(
        "http://{}/api/v1/namespaces/default/pods/p/log?follow=true&tailLines=1",
        server.local_addr()
    );
    let mut resp = reqwest::get(url).await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    assert_eq!(next_chunk(&mut resp).await.as_deref(), Some("two\n"));

    log.text.lock().unwrap().push_str("three\n");
    assert_eq!(next_chunk(&mut resp).await.as_deref(), Some("three\n"));

    log.text.lock().unwrap().push_str("four\nfive\n");
    assert_eq!(next_chunk(&mut resp).await.as_deref(), Some("four\nfive\n"));

    *log.gone.lock().unwrap() = true;
    assert_eq!(
        next_chunk(&mut resp).await,
        None,
        "the stream ends once the log is gone"
    );
}

#[tokio::test]
async fn an_unfollowed_log_is_one_answer() {
    let log = Arc::new(GrowingLog::default());
    *log.text.lock().unwrap() = "one\ntwo\n".into();
    let server = serve(log).await;
    let url = format!(
        "http://{}/api/v1/namespaces/default/pods/p/log?tailLines=1",
        server.local_addr()
    );
    let body = reqwest::get(url).await.unwrap().text().await.unwrap();
    assert_eq!(body, "two\n");
}

//! T4.1 (I30) — the path authz classifies is decoded exactly as axum's `Path`
//! extractor decodes it.
//!
//! [`RequestInfo::parse`] percent-decodes the request path once, and authz
//! judges that decoded path. One route still dispatches through axum's own
//! `Path` extractor (discovery's `/apis/{group}/{version}`), so the two
//! decoders have to agree byte for byte, on every input, including the ones
//! nobody thought to write down: malformed and truncated escapes, `%%`,
//! encoded separators, and escapes that decode to bytes that are not UTF-8.
//!
//! This is a DIFFERENTIAL test against the real extractor, not against the
//! `percent-encoding` crate: it sends each generated path through an axum
//! router whose wildcard handler echoes what `Path` produced, and compares
//! that with what `RequestInfo` carries for the same URI. It fails the day
//! either side moves, whichever one it is.

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{Method, Request, StatusCode, Uri};
use axum::routing::get;
use engenho_apiserver::{RequestInfo, RequestInfoError};
use http_body_util::BodyExt;
use proptest::prelude::*;
use tower::ServiceExt;

/// The route prefix. It ends in `/` and holds no `%`, so no escape can span
/// the boundary between it and the generated tail, and a path under it is
/// never a resource path.
const PREFIX: &str = "/probe/";

/// What axum's `Path` extractor made of `path`: `Ok(decoded tail)` when it
/// accepted it, `Err(status)` when it refused.
async fn axum_decodes(path: &str) -> Result<String, StatusCode> {
    let app = Router::new().route(
        "/probe/*rest",
        get(|Path(rest): Path<String>| async { rest }),
    );
    let req = Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .expect("request");
    let resp = app.oneshot(req).await.expect("infallible router");
    let status = resp.status();
    let body = resp.into_body().collect().await.expect("body").to_bytes();
    if status == StatusCode::OK {
        Ok(String::from_utf8(body.to_vec()).expect("an echoed String is UTF-8"))
    } else {
        Err(status)
    }
}

/// What `RequestInfo` made of `path`: `Ok(decoded tail)` or the parse error.
fn request_info_decodes(path: &str) -> Result<String, RequestInfoError> {
    let uri: Uri = path.parse().expect("generated paths are valid URIs");
    let ri = RequestInfo::parse(&Method::GET, &uri)?;
    let decoded = ri
        .non_resource_url()
        .expect("a /probe/ path is not a resource path");
    Ok(decoded
        .strip_prefix(PREFIX)
        .expect("decoding never touches the prefix")
        .to_string())
}

/// One piece of a generated path tail: a plain path character, or one of the
/// escape shapes a decoder can get wrong.
fn token() -> impl Strategy<Value = String> {
    prop_oneof![
        // Unreserved and sub-delim path characters, `/` and `+` included.
        prop::sample::select(vec![
            "a", "Z", "0", "9", "-", ".", "_", "~", "/", "+", "!", "$", "&", "'", "(", ")", "*",
            ",", ";", "=", ":", "@",
        ])
        .prop_map(str::to_string),
        // A well-formed escape of ANY byte, upper- or lower-case hex. Bytes
        // >= 0x80 build UTF-8 sequences, valid or not.
        (any::<u8>(), any::<bool>()).prop_map(|(b, upper)| {
            let hex = |n: u8| {
                let c = char::from_digit(u32::from(n), 16).expect("a nibble");
                if upper { c.to_ascii_uppercase() } else { c }
            };
            ['%', hex(b >> 4), hex(b & 0xf)].iter().collect()
        }),
        // A bare `%`, a truncated escape, and a non-hex escape.
        Just("%".to_string()),
        prop::sample::select(vec!["%2", "%f", "%A"]).prop_map(str::to_string),
        prop::sample::select(vec!["%zz", "%g0", "%0g", "%%"]).prop_map(str::to_string),
    ]
}

/// A path under [`PREFIX`]. The tail starts with a literal so the wildcard
/// always has something to capture.
fn probe_path() -> impl Strategy<Value = String> {
    prop::collection::vec(token(), 0..12)
        .prop_map(|tokens| [PREFIX, "x", &tokens.concat()].concat())
}

/// Hand-picked paths that MUST agree whatever the generator draws.
const PINNED: &[&str] = &[
    "/probe/x%2Fy",
    "/probe/x%2fy",
    "/probe/x%252F",
    "/probe/x%E2%9C%93",
    "/probe/x+y",
    "/probe/x%zz",
    "/probe/x%2",
    "/probe/x%",
    "/probe/x%%41",
    "/probe/x%C3%28",
    "/probe/x%FF",
];

/// The two decoders agree on `path`: the same decoded tail, or both refuse
/// (axum with a 400, `RequestInfo` with the error that renders as one).
async fn assert_agree(path: &str) {
    match (axum_decodes(path).await, request_info_decodes(path)) {
        (Ok(axum), Ok(ours)) => assert_eq!(ours, axum, "decoded differently: {path}"),
        (Err(StatusCode::BAD_REQUEST), Err(RequestInfoError::PathNotUtf8)) => {}
        (axum, ours) => panic!("{path}: axum {axum:?}, RequestInfo {ours:?}"),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

#[test]
fn pinned_paths_decode_as_axum_decodes_them() {
    let rt = runtime();
    for path in PINNED {
        rt.block_on(assert_agree(path));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn every_generated_path_decodes_as_axum_decodes_it(path in probe_path()) {
        runtime().block_on(assert_agree(&path));
    }
}

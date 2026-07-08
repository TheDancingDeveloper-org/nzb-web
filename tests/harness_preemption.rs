//! Integration tests for queue preemption and automatic resumption.

mod harness;

use std::collections::HashMap;
use std::time::Duration;

use harness::nzb_fixture::NzbFixture;
use harness::{HarnessBuilder, ServerProfile, yenc_articles};
use nzb_nntp::testutil::MockConfig;
use nzb_web::nzb_core::models::{JobStatus, Priority};

fn make_fixture(prefix: &str, segments: usize) -> (Vec<u8>, HashMap<String, Vec<u8>>) {
    let mids: Vec<String> = (1..=segments)
        .map(|idx| format!("{prefix}-{idx}@test"))
        .collect();
    let bodies: Vec<Vec<u8>> = (1..=segments)
        .map(|idx| format!("{prefix}-body-{idx}").into_bytes())
        .collect();
    let segs: Vec<(&str, &[u8])> = mids
        .iter()
        .zip(bodies.iter())
        .map(|(mid, body)| (mid.as_str(), body.as_slice()))
        .collect();
    let file_name = format!("{prefix}.bin");
    let built = NzbFixture::new(prefix).add_file(&file_name, &segs).build();
    let triples: Vec<(&str, &[u8], &str)> = built
        .articles
        .iter()
        .map(|(mid, body, file)| (*mid, *body, file.as_str()))
        .collect();
    (built.xml, yenc_articles(&triples))
}

#[tokio::test]
async fn preempted_job_resumes_after_high_priority_job_finishes() {
    let (slow_xml, slow_articles) = make_fixture("slow-job", 8);
    let (fast_xml, fast_articles) = make_fixture("fast-job", 1);
    let mut articles = slow_articles;
    articles.extend(fast_articles);

    let server = ServerProfile::start(
        "preempt-srv",
        MockConfig {
            articles,
            response_delay: Some(Duration::from_millis(250)),
            ..Default::default()
        },
        1,
    )
    .await
    .with_priority(0);

    let engine = HarnessBuilder::new()
        .with_server(server)
        .max_active_downloads(1)
        .article_timeout(10)
        .build();

    let slow_id = engine
        .submit_nzb_xml("slow-job", slow_xml)
        .expect("submit slow job");
    assert!(
        engine
            .wait_for(Duration::from_secs(5), |snap| {
                snap.job(&slow_id)
                    .map(|job| job.status == JobStatus::Downloading)
                    .unwrap_or(false)
            })
            .await,
        "slow job never started downloading"
    );

    let fast_id = engine
        .submit_nzb_xml("fast-job", fast_xml)
        .expect("submit fast job");
    engine
        .queue_manager
        .set_job_priority(&fast_id, Priority::High)
        .expect("promote fast job");

    assert!(
        engine
            .wait_for(Duration::from_secs(5), |snap| {
                let slow_paused = snap
                    .job(&slow_id)
                    .map(|job| job.status == JobStatus::Paused)
                    .unwrap_or(false);
                let fast_active = snap
                    .job(&fast_id)
                    .map(|job| job.status == JobStatus::Downloading)
                    .unwrap_or(false);
                slow_paused && fast_active
            })
            .await,
        "preemption never paused the slow job and started the fast job"
    );

    tokio::time::sleep(Duration::from_secs(7)).await;

    assert!(
        engine
            .wait_for(Duration::from_secs(5), |snap| {
                snap.job(&slow_id)
                    .map(|job| job.status == JobStatus::Downloading)
                    .unwrap_or(false)
            })
            .await,
        "preempted slow job did not resume after the high-priority job finished"
    );
}

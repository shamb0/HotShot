use std::{sync::Arc, time::Duration};

use async_compatibility_layer::art::async_timeout;
use futures::StreamExt;
use hotshot::tasks::task_state::CreateTaskState;
use hotshot_example_types::{
    block_types::TestTransaction,
    node_types::{MemoryImpl, TestTypes},
};
use hotshot_macros::{run_test, test_scripts};
use hotshot_task_impls::{da::DaTaskState, events::HotShotEvent};
use hotshot_testing::{
    helpers::build_system_handle_from_launcher,
    predicates::{
        event::{exact, expect_external_events, ext_event_exact},
        PredicateResult,
    },
    script::{Expectations, InputOrder, TaskScript},
    serial,
    test_builder::TestDescription,
    view_generator::TestViewGenerator,
};
use hotshot_types::{
    data::ViewNumber,
    event::{Event, EventType},
    simple_vote::DaData,
    traits::{
        block_contents::precompute_vid_commitment, election::Membership,
        node_implementation::ConsensusTime,
    },
};

#[cfg_attr(async_executor_impl = "tokio", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(async_executor_impl = "async-std", async_std::test)]
async fn test_da_task_outdated_proposal() {
    async_compatibility_layer::logging::setup_logging();
    async_compatibility_layer::logging::setup_backtrace();

    let node_id: u64 = 2;
    let num_nodes: usize = 10;
    let da_committee_size: usize = 7;

    let mut test_description = TestDescription::default();
    test_description.num_nodes_with_stake = num_nodes;
    test_description.da_staked_committee_size = da_committee_size;
    test_description.start_nodes = num_nodes;

    let launcher = test_description
        .gen_launcher(node_id)
        .modify_default_config(|config| {
            config.next_view_timeout = 1000;
            config.timeout_ratio = (12, 10);
            config.da_staked_committee_size = da_committee_size;
        });

    let (handle, _event_sender, _event_receiver) =
        build_system_handle_from_launcher::<TestTypes, MemoryImpl>(node_id, launcher, None)
            .await
            .expect("Failed to initialize HotShot");

    // Make some empty encoded transactions, we just care about having a commitment handy for the
    // later calls. We need the VID commitment to be able to propose later.
    let transactions = vec![TestTransaction::new(vec![0])];
    let encoded_transactions = Arc::from(TestTransaction::encode(&transactions));
    let (payload_commit, _precompute) = precompute_vid_commitment(
        &encoded_transactions,
        handle.hotshot.memberships.quorum_membership.total_nodes(),
    );

    let mut view_generator = TestViewGenerator::generate(
        handle.hotshot.memberships.quorum_membership.clone(),
        handle.hotshot.memberships.da_membership.clone(),
    );

    // Generate views
    let view1 = view_generator.next().await.unwrap();
    let _view2 = view_generator.next().await.unwrap();

    view_generator.add_transactions(transactions);

    let view3 = view_generator.next().await.unwrap();

    // Prepare inputs
    let inputs = vec![
        serial![
            HotShotEvent::ViewChange(ViewNumber::new(1)),
            HotShotEvent::ViewChange(ViewNumber::new(2)),
            HotShotEvent::ViewChange(ViewNumber::new(3)),
            // Send an outdated proposal (view 1) when we're in view 3
            HotShotEvent::DaProposalRecv(view1.da_proposal.clone(), view1.leader_public_key),
        ],
        serial![
            // Send a current proposal (view 3)
            HotShotEvent::DaProposalRecv(view3.da_proposal.clone(), view3.leader_public_key),
        ],
    ];

    let expectations = vec![
        Expectations::from_outputs(vec![]), // Expect no output for the outdated proposal
        Expectations::from_outputs(vec![
            exact(HotShotEvent::DaProposalValidated(
                view3.da_proposal.clone(),
                view3.leader_public_key,
            )),
            exact(HotShotEvent::DaVoteSend(view3.create_da_vote(
                DaData {
                    payload_commit: payload_commit,
                },
                &handle,
            ))),
        ]),
    ];

    let external_event_expectations = vec![expect_external_events(vec![ext_event_exact(Event {
        view_number: view3.view_number,
        event: EventType::DaProposal {
            proposal: view3.da_proposal.clone(),
            sender: view3.leader_public_key,
        },
    })])];

    let da_state = DaTaskState::<TestTypes, MemoryImpl>::create_from(&handle).await;
    let mut da_script = TaskScript {
        timeout: Duration::from_millis(100),
        state: da_state,
        expectations: expectations,
    };

    let mut output_event_stream_recv = handle.event_stream();

    run_test![inputs, da_script].await;

    // Check external events
    let mut external_event_expectations_met = vec![false; external_event_expectations.len()];

    while let Ok(Some(ext_event_received_output)) =
        async_timeout(da_script.timeout, output_event_stream_recv.next()).await
    {
        tracing::debug!("Test received Ext Event: {:?}", ext_event_received_output);

        for (index, expectation) in external_event_expectations.iter().enumerate() {
            if !external_event_expectations_met[index] {
                for predicate in &expectation.output_asserts {
                    if predicate
                        .evaluate(&Arc::new(ext_event_received_output.clone()))
                        .await
                        == PredicateResult::Pass
                    {
                        external_event_expectations_met[index] = true;
                        break;
                    }
                }
            }
        }

        // Check if all expectations are met
        if external_event_expectations_met.iter().all(|&x| x) {
            break;
        }
    }

    // Assert that all external event expectations were met
    assert!(
        external_event_expectations_met.iter().all(|&x| x),
        "Not all external event expectations were met"
    );
}

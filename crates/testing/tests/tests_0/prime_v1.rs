use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use hotshot::tasks::task_state::CreateTaskState;
use hotshot_example_types::{
    block_types::TestTransaction,
    node_types::{MemoryImpl, TestTypes},
};
use hotshot_macros::{run_test, test_scripts};
use hotshot_task_impls::{da::DaTaskState, events::HotShotEvent};
use hotshot_testing::{
    helpers::{build_system_handle_from_launcher, check_external_events},
    predicates::event::{exact, expect_external_events, ext_event_exact},
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

/// Test the DA Task for handling an outdated proposal.
/// 
/// This test checks that when an outdated DA proposal is received, it doesn't produce 
/// any output, while a current proposal triggers appropriate actions (validation and voting).
#[cfg_attr(async_executor_impl = "tokio", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(async_executor_impl = "async-std", async_std::test)]
async fn test_da_task_outdated_proposal() {
    // Setup logging and backtrace for debugging
    async_compatibility_layer::logging::setup_logging();
    async_compatibility_layer::logging::setup_backtrace();

    // Parameters for the test
    let node_id: u64 = 2;
    let num_nodes: usize = 10;
    let da_committee_size: usize = 7;

    // Initialize test description with node and committee details
    let mut test_description = TestDescription::default();
    test_description.num_nodes_with_stake = num_nodes;
    test_description.da_staked_committee_size = da_committee_size;
    test_description.start_nodes = num_nodes;

    // Generate a launcher for the test system with a custom configuration
    let launcher = test_description
        .gen_launcher(node_id)
        .modify_default_config(|config| {
            config.next_view_timeout = 1000;
            config.timeout_ratio = (12, 10);
            config.da_staked_committee_size = da_committee_size;
        });

    // Build the system handle using the launcher configuration
    let (handle, _event_sender, _event_receiver) =
        build_system_handle_from_launcher::<TestTypes, MemoryImpl>(node_id, launcher, None)
            .await
            .expect("Failed to initialize HotShot");

    // Prepare empty transactions and compute a commitment for later use
    let transactions = vec![TestTransaction::new(vec![0])];
    let encoded_transactions = Arc::from(TestTransaction::encode(&transactions));
    let (payload_commit, _precompute) = precompute_vid_commitment(
        &encoded_transactions,
        handle.hotshot.memberships.quorum_membership.total_nodes(),
    );

    // Initialize a view generator using the current memberships
    let mut view_generator = TestViewGenerator::generate(
        handle.hotshot.memberships.quorum_membership.clone(),
        handle.hotshot.memberships.da_membership.clone(),
    );

    // Generate views for the test
    let view1 = view_generator.next().await.unwrap();
    let _view2 = view_generator.next().await.unwrap();
    view_generator.add_transactions(transactions);
    let view3 = view_generator.next().await.unwrap();

    // Define input events for the test:
    // 1. Three view changes and an outdated proposal for view 1 when in view 3
    // 2. A current proposal for view 3
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

    // Define expectations:
    // 1. No output for the outdated proposal
    // 2. Validation and voting actions for the current proposal
    let expectations = vec![
        Expectations::from_outputs(vec![]),
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

    // Define expectations for external events triggered by the system
    let external_event_expectations = vec![expect_external_events(vec![ext_event_exact(Event {
        view_number: view3.view_number,
        event: EventType::DaProposal {
            proposal: view3.da_proposal.clone(),
            sender: view3.leader_public_key,
        },
    })])];

    // Create DA task state and script for the test
    let da_state = DaTaskState::<TestTypes, MemoryImpl>::create_from(&handle).await;
    let mut da_script = TaskScript {
        timeout: Duration::from_millis(100),
        state: da_state,
        expectations: expectations,
    };

    // Run the test with the inputs and check the resulting events
    let output_event_stream_recv = handle.event_stream();
    run_test![inputs, da_script].await;

    // Validate the external events against expectations
    let result = check_external_events(
        output_event_stream_recv,
        &external_event_expectations,
        da_script.timeout,
    )
    .await;
    assert!(result.is_ok(), "{}", result.err().unwrap());
}

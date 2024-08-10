use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use hotshot::tasks::task_state::CreateTaskState;
use hotshot_example_types::{
    block_types::{TestMetadata, TestTransaction},
    node_types::{MemoryImpl, TestTypes},
};
use hotshot_macros::{run_test, test_scripts};
use hotshot_task_impls::{da::DaTaskState, events::HotShotEvent::*};
use hotshot_testing::{
    helpers::build_system_handle_from_launcher,
    predicates::event::exact,
    script::{Expectations, InputOrder, TaskScript},
    serial,
    test_builder::TestDescription,
    view_generator::TestViewGenerator,
};
use hotshot_types::{
    constants::BaseVersion,
    data::{null_block, PackedBundle, ViewNumber},
    simple_vote::DaData,
    traits::{
        block_contents::precompute_vid_commitment, election::Membership,
        node_implementation::ConsensusTime,
    },
};
use vbs::version::StaticVersionType;

#[cfg_attr(async_executor_impl = "tokio", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(async_executor_impl = "async-std", async_std::test)]
async fn test_da_task_with_custom_config() {
    async_compatibility_layer::logging::setup_logging();
    async_compatibility_layer::logging::setup_backtrace();

    let node_id: u64 = 2;
    let num_nodes: usize = 10;
    let da_committee_size: usize = 7;

    let mut test_description = TestDescription::default();
    test_description.num_nodes_with_stake = num_nodes;
    test_description.da_staked_committee_size = da_committee_size;
    test_description.start_nodes = num_nodes;
    // Add more customizations as needed

    let launcher = test_description
        .gen_launcher(node_id)
        .modify_default_config(|config| {
            config.next_view_timeout = 1000;
            config.timeout_ratio = (12, 10);
            config.da_staked_committee_size = da_committee_size;
            // Modify other config parameters as needed
        });

    let (handle, _event_sender, _event_receiver) =
        build_system_handle_from_launcher::<TestTypes, MemoryImpl>(node_id, launcher, None)
            .await
            .expect("Failed to initialize HotShot");

    // Make some empty encoded transactions, we just care about having a commitment handy for the
    // later calls. We need the VID commitment to be able to propose later.
    let transactions = vec![TestTransaction::new(vec![0])];
    let encoded_transactions = Arc::from(TestTransaction::encode(&transactions));
    let (payload_commit, precompute) = precompute_vid_commitment(
        &encoded_transactions,
        handle.hotshot.memberships.quorum_membership.total_nodes(),
    );

    let mut view_generator = TestViewGenerator::generate(
        handle.hotshot.memberships.quorum_membership.clone(),
        handle.hotshot.memberships.da_membership.clone(),
    );

    let mut proposals = Vec::new();
    let mut leaders = Vec::new();
    let mut votes = Vec::new();
    let mut dacs = Vec::new();
    let mut vids = Vec::new();

    for view in (&mut view_generator).take(1).collect::<Vec<_>>().await {
        proposals.push(view.da_proposal.clone());
        leaders.push(view.leader_public_key);
        votes.push(view.create_da_vote(DaData { payload_commit }, &handle));
        dacs.push(view.da_certificate.clone());
        vids.push(view.vid_proposal.clone());
    }

    view_generator.add_transactions(vec![TestTransaction::new(vec![0])]);

    for view in (&mut view_generator).take(1).collect::<Vec<_>>().await {
        proposals.push(view.da_proposal.clone());
        leaders.push(view.leader_public_key);
        votes.push(view.create_da_vote(DaData { payload_commit }, &handle));
        dacs.push(view.da_certificate.clone());
        vids.push(view.vid_proposal.clone());
    }

    // Generate test inputs & expectations
    let inputs = vec![
        serial![
            ViewChange(ViewNumber::new(1)),
            ViewChange(ViewNumber::new(2)),
            BlockRecv(PackedBundle::new(
                encoded_transactions,
                TestMetadata,
                ViewNumber::new(2),
                vec1::vec1![null_block::builder_fee(
                    handle
                        .hotshot
                        .memberships
                        .quorum_membership
                        .clone()
                        .total_nodes(),
                    BaseVersion::version()
                )
                .unwrap()],
                Some(precompute),
                None,
            )),
        ],
        serial![DaProposalRecv(proposals[1].clone(), leaders[1])],
    ];

    let expectations = vec![
        Expectations::from_outputs(vec![exact(DaProposalSend(
            proposals[1].clone(),
            leaders[1],
        ))]),
        Expectations::from_outputs(vec![
            exact(DaProposalValidated(proposals[1].clone(), leaders[1])),
            exact(DaVoteSend(votes[1].clone())),
        ]),
    ];

    let da_state = DaTaskState::<TestTypes, MemoryImpl>::create_from(&handle).await;
    let mut da_script = TaskScript {
        timeout: Duration::from_millis(100),
        state: da_state,
        expectations: expectations,
    };

    run_test![inputs, da_script].await;
}

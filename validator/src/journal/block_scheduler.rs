/*
 * Copyright 2018 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ------------------------------------------------------------------------------
 */

use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use crate::block::Block;
use crate::journal::block_validator::{BlockStatusStore, BlockValidationResult};
use crate::journal::block_wrapper::BlockStatus;
use crate::journal::chain::COMMIT_STORE;
use crate::journal::{block_manager::BlockManager, NULL_BLOCK_IDENTIFIER};
use crate::metrics;
use lazy_static::lazy_static;
use log::{debug, info, warn};

lazy_static! {
    static ref COLLECTOR: metrics::MetricsCollectorHandle =
        metrics::get_collector("sawtooth_validator.block_validator");
}

#[derive(Clone)]
pub struct BlockScheduler<B: BlockStatusStore> {
    state: Arc<Mutex<BlockSchedulerState<B>>>,
}

impl<B: BlockStatusStore> BlockScheduler<B> {
    //call from chain::fail_block
    ///only meant to be called from the chain controller.
    pub fn insert_into_processing(&self, block_id: String) {
        self.state
            .lock()
            .expect("The BlockScheduler Mutex was poisoned")
            .processing
            .insert(block_id);
    }

    pub fn new(block_manager: BlockManager, block_status_store: B) -> Self {
        BlockScheduler {
            state: Arc::new(Mutex::new(BlockSchedulerState {
                block_manager,
                block_status_store,
                pending: HashSet::new(),
                processing: HashSet::new(),
                descendants_by_previous_id: HashMap::new(),
                results_sender: None,
            })),
        }
    }

    pub fn set_results_sender(&self, sender: Sender<BlockValidationResult>) {
        self.state
            .lock()
            .expect("The BlockScheduler Mutex was poisoned")
            .results_sender = Some(sender);
    }

    /// Schedule the blocks, returning those that are directly ready to
    /// validate
    pub fn schedule(&self, blocks: Vec<Block>) -> Vec<Block> {
        self.state
            .lock()
            .expect("The BlockScheduler Mutex was poisoned")
            .schedule(blocks)
    }

    /// Mark the block associated with block_id as having completed block
    /// validation, returning any descendants marked for processing.
    /// Will remove block_id from processing, take all descendants, and move
    /// them to processing.
    pub fn done(&self, block_id: &str, mark_descendants_invalid: bool) -> Vec<Block> {
        self.state
            .lock()
            .expect("The BlockScheduler Mutex was poisoned")
            .done(block_id, mark_descendants_invalid)
    }

    pub fn contains(&self, block_id: &str) -> bool {
        self.state
            .lock()
            .expect("The BlockScheduler Mutex was poisoned")
            .contains(block_id)
    }
}

struct BlockSchedulerState<B: BlockStatusStore> {
    pub block_manager: BlockManager,
    pub block_status_store: B,
    pub pending: HashSet<String>,
    pub processing: HashSet<String>,
    pub descendants_by_previous_id: HashMap<String, Vec<Block>>,
    results_sender: Option<Sender<BlockValidationResult>>,
}

impl<B: BlockStatusStore> BlockSchedulerState<B> {
    fn schedule(&mut self, blocks: Vec<Block>) -> Vec<Block> {
        let mut ready = vec![];
        for block in blocks {
            if self.processing.contains(&block.header_signature) {
                debug!(
                    "During block scheduling, block already in process: {}",
                    &block.header_signature
                );
                continue;
            }

            if self.pending.contains(&block.header_signature) {
                debug!(
                    "During block scheduling, block already in pending: {}",
                    &block.header_signature
                );
                continue;
            }

            if self.processing.contains(&block.previous_block_id) {
                debug!(
                    "During block scheduling, previous block {} in process, adding block {} to pending",
                    &block.previous_block_id,
                    &block.header_signature);
                self.add_block_to_pending(block);
                continue;
            }

            if self.pending.contains(&block.previous_block_id) {
                debug!(
                    "During block scheduling, previous block {} is pending, adding block {} to pending",
                    &block.previous_block_id,
                    &block.header_signature);

                self.add_block_to_pending(block);
                continue;
            }

            //up to this point block and pred are not in validation
            if block.previous_block_id == NULL_BLOCK_IDENTIFIER {
                debug!("Adding block {} for processing", &block.header_signature);
                self.processing.insert(block.header_signature.clone());
                ready.push(block);
                return ready;
            }

            let prev_block_validity = self.block_validity(&block.previous_block_id);

            match prev_block_validity {
                BlockStatus::Valid => {
                    debug!("Adding block {} for processing", &block.header_signature);

                    self.processing.insert(block.header_signature.clone());
                    ready.push(block);
                }
                // pred results not found though
                BlockStatus::Unknown => {
                    info!(
                    "During block scheduling, predecessor of block {}, {}, status is unknown. Scheduling all blocks since last predecessor with known status",
                    &block.header_signature, &block.previous_block_id);

                    let blocks_previous_to_previous = self.block_manager
                        .branch(&block.previous_block_id)
                        .expect("Block id of block previous to block being scheduled is unknown to the block manager");
                    self.add_block_to_pending(block);

                    let mut to_be_scheduled = vec![];
                    for predecessor in blocks_previous_to_previous {
                        let cache_status = self
                            .block_status_store
                            .status(&predecessor.header_signature);

                        //proxy for is_in_validation
                        if self.contains(&predecessor.header_signature)
                            || cache_status != BlockStatus::Unknown
                        {
                            break;
                        }

                        match self.block_manager.ref_block(&predecessor.header_signature) {
                            Ok(_) => (),
                            Err(err) => {
                                warn!(
                                "Failed to ref block {} during cache-miss block rescheduling: {:?}",
                                &predecessor.header_signature, err
                            );
                            }
                        }

                        to_be_scheduled.push(predecessor);
                    }

                    to_be_scheduled.reverse();

                    for block in self.schedule(to_be_scheduled) {
                        if !ready.contains(&block) {
                            self.processing.insert(block.header_signature.clone());
                            ready.push(block);
                        }
                    }
                }
                BlockStatus::Invalid => {
                    //readily send invalid results to results thread
                    //insert in processing, so that it gets descheduled and its descendants,
                    //descendants must be inserted so that they can be descheduled; handled by recursive schedule
                    self.processing.insert(block.header_signature.clone());
                    self.results_sender.as_ref()
                        .expect("Results' tx is not supposed to be None")
                        .send(BlockValidationResult {
                            block_id: block.header_signature.clone(),
                            execution_results: vec![],
                            num_transactions: 0,
                            status: BlockStatus::Invalid,
                        })
                        .expect("Failed to send invalid block to results thread in the chain controller");

                    debug!(
                        "Block {} has invalid predecessor {}, propagating invalidation",
                        &block.header_signature, &block.previous_block_id
                    );
                }
                BlockStatus::Missing | BlockStatus::InValidation => {
                    warn!(
                        "Block {} has unreachable predecessor {} status {:?}, not scheduling",
                        &block.header_signature,
                        &block.previous_block_id[..8],
                        &prev_block_validity
                    );
                }
            }
        }
        self.update_gauges();
        ready
    }

    fn block_validity(&self, block_id: &str) -> BlockStatus {
        let status = self.block_status_store.status(block_id);
        if status == BlockStatus::Unknown {
            match self
                .block_manager
                .get_from_blockstore(block_id, COMMIT_STORE)
            {
                Err(err) => {
                    warn!("Error during checking block validity: {:?}", err);
                    BlockStatus::Unknown
                }
                Ok(None) => BlockStatus::Unknown,
                Ok(Some(_)) => BlockStatus::Valid,
            }
        } else {
            status
        }
    }

    /// Remove from processing and move all descendants from pending to processing.
    /// When a block is marked invalid, thus all descendants are invalid, do not process them.
    fn done(&mut self, block_id: &str, mark_descendants_invalid: bool) -> Vec<Block> {
        self.processing.remove(block_id);
        let ready = self
            .descendants_by_previous_id
            .remove(block_id)
            .unwrap_or_default();

        for blk in &ready {
            self.pending.remove(&blk.header_signature);
            if !mark_descendants_invalid {
                self.processing.insert(blk.header_signature.clone());
            } else {
                info!(
                    "Predecessor {} marked invalid, marking descendant {} invalid",
                    block_id, &blk.header_signature
                );
            }
        }

        self.update_gauges();
        ready
    }

    fn contains(&self, block_id: &str) -> bool {
        self.pending.contains(block_id) || self.processing.contains(block_id)
    }

    ///insert into pending and get back the pred's descendants, if its is not already there,
    /// insert it.
    fn add_block_to_pending(&mut self, block: Block) {
        self.pending.insert(block.header_signature.clone());
        if let Some(ref mut waiting_descendants) = self
            .descendants_by_previous_id
            .get_mut(&block.previous_block_id)
        {
            if !waiting_descendants.contains(&block) {
                waiting_descendants.push(block);
            }
            return;
        }

        self.descendants_by_previous_id
            .insert(block.previous_block_id.clone(), vec![block]);
    }

    fn update_gauges(&self) {
        let mut blocks_processing = COLLECTOR.gauge("BlockScheduler.blocks_processing", None, None);
        blocks_processing.set_value(self.processing.len());
        let mut blocks_pending = COLLECTOR.gauge("BlockScheduler.blocks_pending", None, None);
        blocks_pending.set_value(self.pending.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::NULL_BLOCK_IDENTIFIER;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_block_scheduler_simple() {
        let block_manager = BlockManager::new();
        let block_status_store = MockStore::new();
        let block_a = create_block("A", NULL_BLOCK_IDENTIFIER, 0);
        let block_a1 = create_block("A1", "A", 1);
        let block_a2 = create_block("A2", "A", 1);
        let block_b2 = create_block("B2", "A2", 2);

        let block_unknown = create_block("UNKNOWN", "A", 1);
        let block_b = create_block("B", "UNKNOWN", 2);
        block_manager
            .put(vec![block_a.clone(), block_unknown.clone()])
            .expect("The block manager failed to `put` a branch");

        let block_scheduler = BlockScheduler::new(block_manager, block_status_store);

        assert_eq!(
            block_scheduler.schedule(vec![
                block_a.clone(),
                block_a1.clone(),
                block_a2.clone(),
                block_b2.clone(),
            ]),
            vec![block_a.clone()]
        );

        assert_eq!(
            block_scheduler.done(&block_a.header_signature, false),
            vec![block_a1, block_a2]
        );

        assert_eq!(block_scheduler.schedule(vec![block_b]), vec![block_unknown]);
    }

    #[test]
    fn test_block_scheduler_multiple_forks() {
        let block_manager = BlockManager::new();
        let block_status_store: Arc<Mutex<HashMap<String, BlockStatus>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let block_a = create_block("A", NULL_BLOCK_IDENTIFIER, 0);
        let block_b = create_block("B", "A", 1);
        let block_c1 = create_block("C1", "B", 2);
        let block_c2 = create_block("C2", "B", 2);
        let block_c3 = create_block("C3", "B", 2);
        let block_d1 = create_block("D11", "C1", 3);
        let block_d2 = create_block("D12", "C1", 3);
        let block_d3 = create_block("D13", "C1", 3);

        block_manager
            .put(vec![
                block_a.clone(),
                block_b.clone(),
                block_c1.clone(),
                block_d1.clone(),
            ])
            .expect("The block manager failed to `put` a branch");
        block_manager
            .put(vec![block_b.clone(), block_c2.clone()])
            .expect("The block manager failed to put a branch");

        block_manager
            .put(vec![block_b.clone(), block_c3.clone()])
            .expect("The block manager failed to put a block");

        block_manager
            .put(vec![block_c1.clone(), block_d2.clone()])
            .expect("The block manager failed to `put` a branch");

        block_manager
            .put(vec![block_c1.clone(), block_d3.clone()])
            .expect("The block manager failed to put a branch");

        let block_scheduler = BlockScheduler::new(block_manager, block_status_store);

        assert_eq!(
            block_scheduler.schedule(vec![block_a.clone()]),
            vec![block_a.clone()],
            "The genesis block's predecessor does not need to be validated"
        );

        assert_eq!(
            block_scheduler.schedule(vec![
                block_b.clone(),
                block_c1.clone(),
                block_c2.clone(),
                block_c3.clone(),
            ]),
            vec![],
            "Block A has not been validated yet"
        );

        assert_eq!(
            block_scheduler.done(&block_a.header_signature, false),
            vec![block_b.clone()],
            "Marking Block A as complete, makes Block B available"
        );

        assert_eq!(
            block_scheduler.schedule(vec![block_d1.clone(), block_d2.clone(), block_d3.clone()]),
            vec![],
            "None of Blocks D1, D2, D3 are available"
        );

        assert_eq!(
            block_scheduler.done(&block_b.header_signature, false),
            vec![block_c1.clone(), block_c2.clone(), block_c3.clone()],
            "Marking Block B as complete, makes Block C1, C2, C3 available"
        );

        assert_eq!(
            block_scheduler.done(&block_c2.header_signature, false),
            vec![],
            "No Blocks are available"
        );

        assert_eq!(
            block_scheduler.done(&block_c3.header_signature, false),
            vec![],
            "No Blocks are available"
        );

        assert_eq!(
            block_scheduler.done(&block_c1.header_signature, false),
            vec![block_d1.clone(), block_d2.clone(), block_d3.clone()],
            "Blocks D1, D2, D3 are available"
        );
    }

    #[test]
    fn test_cache_misses() {
        let block_manager = BlockManager::new();
        let block_status_store: Arc<Mutex<HashMap<String, BlockStatus>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let block_a = create_block("A", NULL_BLOCK_IDENTIFIER, 0);
        let block_b = create_block("B", "A", 1);
        let block_c1 = create_block("C1", "B", 2);
        let block_c2 = create_block("C2", "B", 2);
        let block_c3 = create_block("C3", "B", 2);

        block_manager
            .put(vec![block_a.clone(), block_b.clone(), block_c1.clone()])
            .expect("Block manager errored trying to put a branch");

        block_manager
            .put(vec![block_b.clone(), block_c2.clone()])
            .expect("Block manager errored trying to put a branch");

        block_manager
            .put(vec![block_b.clone(), block_c3.clone()])
            .expect("Block manager errored trying to put a branch");

        let block_scheduler = BlockScheduler::new(block_manager, Arc::clone(&block_status_store));

        assert_eq!(
            block_scheduler.schedule(vec![block_a.clone(), block_b.clone()]),
            vec![block_a.clone()],
            "Block A is ready, but block b is not"
        );

        block_status_store
            .lock()
            .expect("Mutex was poisoned")
            .insert(block_a.header_signature.clone(), BlockStatus::Valid);

        assert_eq!(
            block_scheduler.done(&block_a.header_signature, false),
            vec![block_b.clone()],
            "Now Block B is ready"
        );

        // We are not inserting a status for block b so there will be a later miss

        assert_eq!(
            block_scheduler.done(&block_b.header_signature, false),
            vec![],
            "Block B is done and there are no further blocks"
        );

        // Now a cache miss

        assert_eq!(
            block_scheduler.schedule(vec![block_c1.clone(), block_c2.clone(), block_c3.clone()]),
            vec![block_b.clone()],
            "Since there was a cache miss, block b must be scheduled again"
        );
    }

    fn create_block(header_signature: &str, previous_block_id: &str, block_num: u64) -> Block {
        Block {
            header_signature: header_signature.into(),
            batches: vec![],
            state_root_hash: "".into(),
            consensus: vec![],
            batch_ids: vec![],
            signer_public_key: "".into(),
            previous_block_id: previous_block_id.into(),
            block_num,
            header_bytes: vec![],
        }
    }

    impl BlockStatusStore for Arc<Mutex<HashMap<String, BlockStatus>>> {
        fn status(&self, block_id: &str) -> BlockStatus {
            self.lock()
                .expect("Mutex was poisoned")
                .get(block_id)
                .cloned()
                .unwrap_or(BlockStatus::Unknown)
        }
    }
    #[derive(Clone)]
    struct MockStore {}

    impl MockStore {
        fn new() -> Self {
            MockStore {}
        }
    }

    impl BlockStatusStore for MockStore {
        fn status(&self, block_id: &str) -> BlockStatus {
            if block_id == "UNKNOWN" {
                return BlockStatus::Unknown;
            }
            BlockStatus::Valid
        }
    }
}

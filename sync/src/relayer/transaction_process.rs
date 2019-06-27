use crate::relayer::Relayer;
use crate::relayer::MAX_RELAY_PEERS;
use ckb_core::{transaction::Transaction, Cycle};
use ckb_logger::debug_target;
use ckb_network::{CKBProtocolContext, PeerIndex, TargetSession};
use ckb_protocol::{RelayMessage, RelayTransaction as FbsRelayTransaction};
use ckb_store::ChainStore;
use failure::Error as FailureError;
use flatbuffers::FlatBufferBuilder;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_BAN_TIME: Duration = Duration::from_secs(3600 * 24 * 3);

pub struct TransactionProcess<'a, CS> {
    message: &'a FbsRelayTransaction<'a>,
    relayer: &'a Relayer<CS>,
    nc: Arc<dyn CKBProtocolContext + Sync>,
    peer: PeerIndex,
}

impl<'a, CS: ChainStore + Sync + 'static> TransactionProcess<'a, CS> {
    pub fn new(
        message: &'a FbsRelayTransaction,
        relayer: &'a Relayer<CS>,
        nc: Arc<CKBProtocolContext + Sync>,
        peer: PeerIndex,
    ) -> Self {
        TransactionProcess {
            message,
            relayer,
            nc,
            peer,
        }
    }

    pub fn execute(self) -> Result<(), FailureError> {
        let (tx, relay_cycles): (Transaction, Cycle) = (*self.message).try_into()?;
        let tx_hash = tx.hash();

        if self.relayer.shared().already_known_tx(&tx_hash) {
            debug_target!(
                crate::LOG_TARGET_RELAY,
                "discarding already known transaction {:#x}",
                tx_hash
            );
            return Ok(());
        }

        // Insert tx_hash into `already_known`
        // Remove tx_hash from `tx_already_asked`
        self.relayer.shared().mark_as_known_tx(tx_hash.clone());
        // Remove tx_hash from `tx_ask_for_set`
        if let Some(peer_state) = self
            .relayer
            .shared()
            .peers()
            .state
            .write()
            .get_mut(&self.peer)
        {
            peer_state.remove_ask_for_tx(&tx_hash);
        }

        let tx_result = self
            .relayer
            .tx_pool_executor
            .verify_and_add_tx_to_pool(tx.to_owned());
        // disconnect peer if cycles mismatch
        match tx_result {
            Ok(cycles) if cycles == relay_cycles => {
                let selected_peers: Vec<PeerIndex> = {
                    let mut known_txs = self.relayer.shared().known_txs();
                    self.nc
                        .connected_peers()
                        .into_iter()
                        .filter(|target_peer| {
                            known_txs.insert(*target_peer, tx_hash.clone())
                                && (self.peer != *target_peer)
                        })
                        .take(MAX_RELAY_PEERS)
                        .collect()
                };

                let fbb = &mut FlatBufferBuilder::new();
                let message = RelayMessage::build_transaction_hash(fbb, &tx_hash);
                fbb.finish(message, None);
                let data = fbb.finished_data().into();
                if let Err(err) = self
                    .nc
                    .filter_broadcast(TargetSession::Multi(selected_peers), data)
                {
                    debug_target!(
                        crate::LOG_TARGET_RELAY,
                        "relayer send TransactionHash error: {:?}",
                        err,
                    );
                }
            }
            Ok(cycles) => {
                debug_target!(
                    crate::LOG_TARGET_RELAY,
                    "peer {} relay wrong cycles tx: {:?} real cycles {} wrong cycles {}",
                    self.peer,
                    tx,
                    cycles,
                    relay_cycles,
                );
                self.nc.ban_peer(self.peer, DEFAULT_BAN_TIME);
            }
            Err(err) => {
                if err.is_bad_tx() {
                    debug_target!(
                        crate::LOG_TARGET_RELAY,
                        "peer {} relay a invalid tx: {:x}, error: {:?}",
                        self.peer,
                        tx_hash,
                        err
                    );
                    use sentry::{capture_message, with_scope, Level};
                    with_scope(
                        |scope| scope.set_fingerprint(Some(&["ckb-sync", "relay-invalid-tx"])),
                        || {
                            capture_message(
                                &format!(
                                    "ban peer {} {:?}, reason: \
                                     relay invalid tx: {:?}, error: {:?}",
                                    self.peer, DEFAULT_BAN_TIME, tx, err
                                ),
                                Level::Info,
                            )
                        },
                    );
                    self.nc.ban_peer(self.peer, DEFAULT_BAN_TIME);
                } else {
                    debug_target!(
                        crate::LOG_TARGET_RELAY,
                        "peer {} relay a conflict or missing input tx: {:x}, error: {:?}",
                        self.peer,
                        tx_hash,
                        err
                    );
                }
            }
        }

        Ok(())
    }
}

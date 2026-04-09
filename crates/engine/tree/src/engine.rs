//! An engine API handler for the chain.

use crate::{
    backfill::BackfillAction,
    chain::{ChainHandler, FromOrchestrator, HandlerEvent},
    download::{BlockDownloader, DownloadAction, DownloadOutcome},
};
use alloy_primitives::B256;
use futures::{Stream, StreamExt};
use reth_chain_state::ExecutedBlockWithTrieUpdates;
use reth_engine_primitives::{BeaconEngineMessage, ConsensusEngineEvent};
use reth_ethereum_primitives::EthPrimitives;
use reth_payload_primitives::PayloadTypes;
use reth_primitives_traits::{Block, NodePrimitives, RecoveredBlock};
use std::{
    collections::HashSet,
    fmt::Display,
    sync::mpsc::Sender,
    task::{ready, Context, Poll},
};
use tokio::sync::mpsc::UnboundedReceiver;

/// A [`ChainHandler`] that advances the chain based on incoming requests (CL engine API).
///
/// This is a general purpose request handler with network access.
/// This type listens for incoming messages and processes them via the configured request handler.
///
/// ## Overview
///
/// This type is an orchestrator for incoming messages and responsible for delegating requests
/// received from the CL to the handler.
///
/// It is responsible for handling the following:
/// - Delegating incoming requests to the [`EngineRequestHandler`].
/// - Advancing the [`EngineRequestHandler`] by polling it and emitting events.
/// - Downloading blocks on demand from the network if requested by the [`EngineApiRequestHandler`].
///
/// The core logic is part of the [`EngineRequestHandler`], which is responsible for processing the
/// incoming requests.
#[derive(Debug)]
pub struct EngineHandler<T, S, D> {
    /// Processes requests.
    ///
    /// This type is responsible for processing incoming requests.
    handler: T,
    /// Receiver for incoming requests (from the engine API endpoint) that need to be processed.
    incoming_requests: S,
    /// A downloader to download blocks on demand.
    downloader: D,
}

impl<T, S, D> EngineHandler<T, S, D> {
    /// Creates a new [`EngineHandler`] with the given handler and downloader and incoming stream of
    /// requests.
    pub const fn new(handler: T, downloader: D, incoming_requests: S) -> Self
    where
        T: EngineRequestHandler,
    {
        Self { handler, incoming_requests, downloader }
    }

    /// Returns a mutable reference to the request handler.
    pub const fn handler_mut(&mut self) -> &mut T {
        &mut self.handler
    }
}

impl<T, S, D> ChainHandler for EngineHandler<T, S, D>
where
    T: EngineRequestHandler<Block = D::Block>,
    S: Stream + Send + Sync + Unpin + 'static,
    <S as Stream>::Item: Into<T::Request>,
    D: BlockDownloader,
{
    type Event = T::Event;

    fn on_event(&mut self, event: FromOrchestrator) {
        // delegate event to the handler
        self.handler.on_event(event.into());
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<HandlerEvent<Self::Event>> {
        loop {
            // drain the handler first
            while let Poll::Ready(ev) = self.handler.poll(cx) {
                match ev {
                    RequestHandlerEvent::HandlerEvent(ev) => {
                        return match ev {
                            HandlerEvent::BackfillAction(target) => {
                                // bubble up backfill sync request
                                self.downloader.on_action(DownloadAction::Clear);
                                Poll::Ready(HandlerEvent::BackfillAction(target))
                            }
                            HandlerEvent::Event(ev) => {
                                // bubble up the event
                                Poll::Ready(HandlerEvent::Event(ev))
                            }
                            HandlerEvent::FatalError => Poll::Ready(HandlerEvent::FatalError),
                        }
                    }
                    RequestHandlerEvent::Download(req) => {
                        // delegate download request to the downloader
                        self.downloader.on_action(DownloadAction::Download(req));
                    }
                }
            }

            // pop the next incoming request
            if let Poll::Ready(Some(req)) = self.incoming_requests.poll_next_unpin(cx) {
                // and delegate the request to the handler
                self.handler.on_event(FromEngine::Request(req.into()));
                // skip downloading in this iteration to allow the handler to process the request
                continue
            }

            // advance the downloader
            if let Poll::Ready(outcome) = self.downloader.poll(cx) {
                if let DownloadOutcome::Blocks(blocks) = outcome {
                    // delegate the downloaded blocks to the handler
                    self.handler.on_event(FromEngine::DownloadedBlocks(blocks));
                }
                continue
            }

            return Poll::Pending
        }
    }
}

/// A type that processes incoming requests (e.g. requests from the consensus layer, engine API,
/// such as newPayload).
///
/// ## Control flow
///
/// Requests and certain updates, such as a change in backfill sync status, are delegated to this
/// type via [`EngineRequestHandler::on_event`]. This type is responsible for processing the
/// incoming requests and advancing the chain and emit events when it is polled.
pub trait EngineRequestHandler: Send + Sync {
    /// Event type this handler can emit
    type Event: Send;
    /// The request type this handler can process.
    type Request;
    /// Type of the block sent in [`FromEngine::DownloadedBlocks`] variant.
    type Block: Block;

    /// Informs the handler about an event from the [`EngineHandler`].
    fn on_event(&mut self, event: FromEngine<Self::Request, Self::Block>);

    /// Advances the handler.
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<RequestHandlerEvent<Self::Event>>;
}

/// An [`EngineRequestHandler`] that processes engine API requests by delegating to an execution
/// task.
///
/// This type is responsible for advancing the chain during live sync (following the tip of the
/// chain).
///
/// It advances the chain based on received engine API requests by delegating them to the tree
/// executor.
///
/// There are two types of requests that can be processed:
///
/// - `on_new_payload`: Executes the payload and inserts it into the tree. These are allowed to be
///   processed concurrently.
/// - `on_forkchoice_updated`: Updates the fork choice based on the new head. These require write
///   access to the database and are skipped if the handler can't acquire exclusive access to the
///   database.
///
/// In case required blocks are missing, the handler will request them from the network, by emitting
/// a download request upstream.
#[derive(Debug)]
pub struct EngineApiRequestHandler<Request, N: NodePrimitives> {
    /// channel to send messages to the tree to execute the payload.
    to_tree: Sender<FromEngine<Request, N::Block>>,
    /// channel to receive messages from the tree.
    from_tree: UnboundedReceiver<EngineApiEvent<N>>,
}

impl<Request, N: NodePrimitives> EngineApiRequestHandler<Request, N> {
    /// Creates a new `EngineApiRequestHandler`.
    pub const fn new(
        to_tree: Sender<FromEngine<Request, N::Block>>,
        from_tree: UnboundedReceiver<EngineApiEvent<N>>,
    ) -> Self {
        Self { to_tree, from_tree }
    }
}

impl<Request, N: NodePrimitives> EngineRequestHandler for EngineApiRequestHandler<Request, N>
where
    Request: Send,
{
    type Event = ConsensusEngineEvent<N>;
    type Request = Request;
    type Block = N::Block;

    fn on_event(&mut self, event: FromEngine<Self::Request, Self::Block>) {
        // delegate to the tree
        if self.to_tree.send(event).is_err() {
            // The engine tree thread has exited; close the receiver so that the next
            // `poll()` call drains any remaining messages and then returns `FatalError`.
            tracing::warn!(target: "engine", "engine tree channel closed, dropping CL request");
            self.from_tree.close();
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<RequestHandlerEvent<Self::Event>> {
        let Some(ev) = ready!(self.from_tree.poll_recv(cx)) else {
            return Poll::Ready(RequestHandlerEvent::HandlerEvent(HandlerEvent::FatalError))
        };

        let ev = match ev {
            EngineApiEvent::BeaconConsensus(ev) => {
                RequestHandlerEvent::HandlerEvent(HandlerEvent::Event(ev))
            }
            EngineApiEvent::BackfillAction(action) => {
                RequestHandlerEvent::HandlerEvent(HandlerEvent::BackfillAction(action))
            }
            EngineApiEvent::Download(action) => RequestHandlerEvent::Download(action),
        };
        Poll::Ready(ev)
    }
}

/// The type for specifying the kind of engine api.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EngineApiKind {
    /// The chain contains Ethereum configuration.
    #[default]
    Ethereum,
    /// The chain contains Optimism configuration.
    OpStack,
}

impl EngineApiKind {
    /// Returns true if this is the ethereum variant
    pub const fn is_ethereum(&self) -> bool {
        matches!(self, Self::Ethereum)
    }

    /// Returns true if this is the ethereum variant
    pub const fn is_opstack(&self) -> bool {
        matches!(self, Self::OpStack)
    }
}

/// The request variants that the engine API handler can receive.
#[derive(Debug)]
pub enum EngineApiRequest<T: PayloadTypes, N: NodePrimitives> {
    /// A request received from the consensus engine.
    Beacon(BeaconEngineMessage<T>),
    /// Request to insert an already executed block, e.g. via payload building.
    InsertExecutedBlock(ExecutedBlockWithTrieUpdates<N>),
}

impl<T: PayloadTypes, N: NodePrimitives> Display for EngineApiRequest<T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Beacon(msg) => msg.fmt(f),
            Self::InsertExecutedBlock(block) => {
                write!(f, "InsertExecutedBlock({:?})", block.recovered_block().num_hash())
            }
        }
    }
}

impl<T: PayloadTypes, N: NodePrimitives> From<BeaconEngineMessage<T>> for EngineApiRequest<T, N> {
    fn from(msg: BeaconEngineMessage<T>) -> Self {
        Self::Beacon(msg)
    }
}

impl<T: PayloadTypes, N: NodePrimitives> From<EngineApiRequest<T, N>>
    for FromEngine<EngineApiRequest<T, N>, N::Block>
{
    fn from(req: EngineApiRequest<T, N>) -> Self {
        Self::Request(req)
    }
}

/// Events emitted by the engine API handler.
#[derive(Debug)]
pub enum EngineApiEvent<N: NodePrimitives = EthPrimitives> {
    /// Event from the consensus engine.
    // TODO(mattsse): find a more appropriate name for this variant, consider phasing it out.
    BeaconConsensus(ConsensusEngineEvent<N>),
    /// Backfill action is needed.
    BackfillAction(BackfillAction),
    /// Block download is needed.
    Download(DownloadRequest),
}

impl<N: NodePrimitives> EngineApiEvent<N> {
    /// Returns `true` if the event is a backfill action.
    pub const fn is_backfill_action(&self) -> bool {
        matches!(self, Self::BackfillAction(_))
    }
}

impl<N: NodePrimitives> From<ConsensusEngineEvent<N>> for EngineApiEvent<N> {
    fn from(event: ConsensusEngineEvent<N>) -> Self {
        Self::BeaconConsensus(event)
    }
}

/// Events received from the engine.
#[derive(Debug)]
pub enum FromEngine<Req, B: Block> {
    /// Event from the top level orchestrator.
    Event(FromOrchestrator),
    /// Request from the engine.
    Request(Req),
    /// Downloaded blocks from the network.
    DownloadedBlocks(Vec<RecoveredBlock<B>>),
}

impl<Req: Display, B: Block> Display for FromEngine<Req, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Event(ev) => write!(f, "Event({ev:?})"),
            Self::Request(req) => write!(f, "Request({req})"),
            Self::DownloadedBlocks(blocks) => {
                write!(f, "DownloadedBlocks({} blocks)", blocks.len())
            }
        }
    }
}

impl<Req, B: Block> From<FromOrchestrator> for FromEngine<Req, B> {
    fn from(event: FromOrchestrator) -> Self {
        Self::Event(event)
    }
}

/// Requests produced by a [`EngineRequestHandler`].
#[derive(Debug)]
pub enum RequestHandlerEvent<T> {
    /// An event emitted by the handler.
    HandlerEvent(HandlerEvent<T>),
    /// Request to download blocks.
    Download(DownloadRequest),
}

/// A request to download blocks from the network.
#[derive(Debug)]
pub enum DownloadRequest {
    /// Download the given set of blocks.
    BlockSet(HashSet<B256>),
    /// Download the given range of blocks.
    BlockRange(B256, u64),
}

impl DownloadRequest {
    /// Returns a [`DownloadRequest`] for a single block.
    pub fn single_block(hash: B256) -> Self {
        Self::BlockSet(HashSet::from([hash]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use alloy_rpc_types_engine::ForkchoiceState;
    use reth_engine_primitives::{BeaconEngineMessage, OnForkChoiceUpdated};
    use reth_errors::RethResult;
    use reth_ethereum_engine_primitives::EthEngineTypes;
    use reth_ethereum_primitives::{Block as EthBlock, EthPrimitives};
    use reth_payload_primitives::EngineApiMessageVersion;
    use std::{
        sync::mpsc,
        task::{Context as TaskContext, Poll},
    };
    use tokio::sync::{mpsc as tokio_mpsc, oneshot};

    /// Helper: build a minimal `EngineApiRequestHandler` for `EthEngineTypes` /
    /// `EthPrimitives`.  Returns the handler together with the raw mpsc *receiver*
    /// so the test can drop it to simulate a dead tree thread, and the tokio
    /// unbounded *sender* so the test can close `from_tree` independently.
    fn make_handler() -> (
        EngineApiRequestHandler<EngineApiRequest<EthEngineTypes, EthPrimitives>, EthPrimitives>,
        mpsc::Receiver<FromEngine<EngineApiRequest<EthEngineTypes, EthPrimitives>, EthBlock>>,
        tokio_mpsc::UnboundedSender<EngineApiEvent<EthPrimitives>>,
    ) {
        let (to_tree_tx, to_tree_rx) = mpsc::channel();
        let (from_tree_tx, from_tree_rx) = tokio_mpsc::unbounded_channel();
        let handler = EngineApiRequestHandler::new(to_tree_tx, from_tree_rx);
        (handler, to_tree_rx, from_tree_tx)
    }

    /// Build a `ForkchoiceUpdated` beacon message together with its response
    /// oneshot receiver.
    fn make_fcu_message() -> (
        BeaconEngineMessage<EthEngineTypes>,
        oneshot::Receiver<RethResult<OnForkChoiceUpdated>>,
    ) {
        let (tx, rx) = oneshot::channel();
        let msg = BeaconEngineMessage::ForkchoiceUpdated {
            state: ForkchoiceState {
                head_block_hash: B256::ZERO,
                safe_block_hash: B256::ZERO,
                finalized_block_hash: B256::ZERO,
            },
            payload_attrs: None,
            version: EngineApiMessageVersion::V1,
            tx,
        };
        (msg, rx)
    }

    // -----------------------------------------------------------------------
    // Test 1 – `on_event` silently discards `SendError` when the tree thread
    // is dead (receiver dropped).  The call must not panic or return an error.
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_event_silently_discards_send_error_when_receiver_dropped() {
        let (mut handler, to_tree_rx, _from_tree_tx) = make_handler();

        // Drop the receiver to simulate the tree thread having exited.
        drop(to_tree_rx);

        let (msg, _rx) = make_fcu_message();
        let event =
            FromEngine::Request(EngineApiRequest::Beacon(msg));

        // This must not panic. The current (buggy) implementation silently
        // swallows the error with `let _ = ...`.  The test documents that the
        // error *is* discarded — it will still pass after the fix only if the
        // fix doesn't panic, but the companion test below will then also
        // exercise the new behaviour.
        handler.on_event(event);
    }

    // -----------------------------------------------------------------------
    // Test 2 – When `to_tree` receiver is dropped the oneshot sender embedded
    // in the `BeaconEngineMessage` is also dropped, so the CL caller's
    // `.await` receives `RecvError` (channel closed) instead of a structured
    // engine response.
    // -----------------------------------------------------------------------
    #[test]
    fn test_dropped_oneshot_sender_when_tree_thread_dead() {
        let (mut handler, to_tree_rx, _from_tree_tx) = make_handler();

        // Kill the tree receiver.
        drop(to_tree_rx);

        let (msg, mut response_rx) = make_fcu_message();
        handler.on_event(FromEngine::Request(EngineApiRequest::Beacon(msg)));

        // The oneshot sender was inside the message which was dropped by the
        // failed `mpsc::Sender::send`.  The receiver must therefore be closed.
        match response_rx.try_recv() {
            Err(oneshot::error::TryRecvError::Closed) => {
                // Expected: the sender was dropped → CL caller gets no response
            }
            Ok(_) => panic!("unexpected response: tree receiver was dropped, no response expected"),
            Err(oneshot::error::TryRecvError::Empty) => {
                panic!("oneshot still open: the message was not dropped as expected")
            }
        }
    }

    // -----------------------------------------------------------------------
    // Test 3 – When `to_tree` receiver is alive the message is forwarded and
    // the oneshot sender is *not* dropped prematurely (control / regression).
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_event_forwards_message_when_receiver_alive() {
        let (mut handler, to_tree_rx, _from_tree_tx) = make_handler();

        let (msg, mut response_rx) = make_fcu_message();
        handler.on_event(FromEngine::Request(EngineApiRequest::Beacon(msg)));

        // The mpsc receiver should have received the message.  Keep it bound so
        // the oneshot::Sender inside is not dropped.
        let _forwarded =
            to_tree_rx.try_recv().expect("message should have been forwarded to tree");

        // The oneshot is still open (no one answered yet, but it was not dropped).
        assert!(
            matches!(response_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "oneshot should still be open: sender was forwarded, not dropped"
        );
    }

    // -----------------------------------------------------------------------
    // Test 4 – `poll()` returns `FatalError` once `from_tree` sender is
    // dropped, but only *after* a poll tick — meaning there is a window where
    // the engine looks alive while silently dropping messages.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn test_poll_returns_fatal_error_after_from_tree_closed() {
        let (mut handler, _to_tree_rx, from_tree_tx) = make_handler();

        // Close the from_tree channel (simulating tree thread exit).
        drop(from_tree_tx);

        // Poll once — must return FatalError.
        let result = std::future::poll_fn(|cx| handler.poll(cx)).await;
        assert!(
            matches!(result, RequestHandlerEvent::HandlerEvent(HandlerEvent::FatalError)),
            "poll() should return FatalError when from_tree is closed"
        );
    }

    // -----------------------------------------------------------------------
    // Test 5 – After the fix: when on_event detects a dead to_tree channel it
    // closes from_tree so that the next poll() returns FatalError even though
    // the from_tree sender is still held by the test.
    // -----------------------------------------------------------------------
    #[test]
    fn test_on_event_closes_from_tree_on_send_error_so_poll_returns_fatal() {
        let (mut handler, to_tree_rx, _from_tree_tx) = make_handler();

        // Drop only the to_tree receiver (tree thread exited).
        drop(to_tree_rx);
        // Keep `_from_tree_tx` alive — without the fix poll() would return Pending.

        let (msg, mut response_rx) = make_fcu_message();
        handler.on_event(FromEngine::Request(EngineApiRequest::Beacon(msg)));

        // The CL caller gets a closed channel — the message was dropped.
        assert!(
            matches!(response_rx.try_recv(), Err(oneshot::error::TryRecvError::Closed)),
            "CL caller's oneshot should be closed (message was dropped on send error)"
        );

        // After the fix, on_event calls from_tree.close(), so poll() now
        // returns FatalError on the very next tick — no more silent-drop window.
        let waker = futures::task::noop_waker();
        let mut cx = TaskContext::from_waker(&waker);
        assert!(
            matches!(
                handler.poll(&mut cx),
                Poll::Ready(RequestHandlerEvent::HandlerEvent(HandlerEvent::FatalError))
            ),
            "poll() must return FatalError after on_event closes from_tree"
        );
    }
}

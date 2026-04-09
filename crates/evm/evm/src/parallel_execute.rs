//! Traits for parallel execution of EVM blocks.

use core::marker::PhantomData;
use std::sync::Arc;

use crate::execute::Executor;
use alloy_evm::{precompiles::DynPrecompile, Database, EvmEnv};
use alloy_primitives::Address;
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock};
use revm::{
    context::TxEnv,
    context_interface::result::{ExecutionResult, HaltReason},
    database::BundleState,
};

/// The `ParallelExecutor` trait defines the interface for executing EVM blocks in parallel.
pub trait ParallelExecutor {
    /// The primitive types used by the executor.
    type Primitives: NodePrimitives;
    /// The error type returned by the executor.
    type Error;

    /// Executes a single block and returns [`BlockExecutionResult`], without the state changes.
    fn execute_one(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>;

    /// Takes the `BundleState` changeset from the State, replacing it with an empty one.
    fn take_bundle(&mut self) -> BundleState;

    /// The size hint of the batch's tracked state size.
    ///
    /// This is used to optimize DB commits depending on the size of the state.
    fn size_hint(&self) -> usize;

    /// Consumes the type and executes the block.
    ///
    /// # Note
    /// Execution happens without any validation of the output.
    ///
    /// # Returns
    /// The output of the block execution.
    fn execute(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionOutput<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        let result = self.execute_one(block)?;
        Ok(BlockExecutionOutput { state: self.take_bundle(), result })
    }

    /// Executes a single system transaction on the executor's own internal state and commits
    /// the resulting state changes immediately.
    ///
    /// The EVM is constructed internally using the executor's `ParallelState` as the DB,
    /// so there is only ONE source of truth. State changes are committed immediately after
    /// execution. The next call will see updated nonces, balances, and storage without any
    /// external bridging.
    ///
    /// `precompiles` allows callers to inject custom precompiles (e.g. mint, BLS) for this
    /// specific transaction, in addition to any executor-level custom precompiles.
    fn transact_system_txn(
        &mut self,
        evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, Self::Error>;

    /// Applies custom precompiled contracts to the executor.
    ///
    /// These precompiles will be available during transaction execution alongside
    /// the standard Ethereum precompiles. This is a no-op by default.
    fn apply_custom_precompiles(&mut self, custom_precompiles: Arc<Vec<(Address, DynPrecompile)>>);
}

/// Wraps a [`Executor`] to provide a [`ParallelExecutor`] implementation.
#[derive(Debug)]
pub struct WrapExecutor<DB: Database, T: Executor<DB>>(pub T, PhantomData<DB>);

impl<DB: Database, T: Executor<DB>> WrapExecutor<DB, T> {
    /// Creates a new `WrapExecutor` from the given executor.
    pub const fn new(executor: T) -> Self {
        Self(executor, PhantomData)
    }
}

impl<DB: Database, T: Executor<DB>> ParallelExecutor for WrapExecutor<DB, T> {
    type Primitives = T::Primitives;
    type Error = T::Error;

    #[inline]
    fn execute_one(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        self.0.execute_one(block)
    }

    #[inline]
    fn take_bundle(&mut self) -> BundleState {
        self.0.take_bundle()
    }

    #[inline]
    fn size_hint(&self) -> usize {
        self.0.size_hint()
    }

    #[inline]
    fn transact_system_txn(
        &mut self,
        evm_env: EvmEnv,
        precompiles: Vec<(Address, DynPrecompile)>,
        tx_env: TxEnv,
    ) -> Result<ExecutionResult<HaltReason>, Self::Error> {
        self.0.transact_system_txn(evm_env, precompiles, tx_env)
    }

    #[inline]
    fn apply_custom_precompiles(
        &mut self,
        _custom_precompiles: Arc<Vec<(Address, DynPrecompile)>>,
    ) {
        // TODO(Ashin Gau): How does basic executor handle custom precompiles
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{execute::Executor, OnStateHook};
    use alloy_evm::{
        precompiles::{DynPrecompile, PrecompileInput},
        EvmEnv,
    };
    use alloy_primitives::{Address, Bytes};
    use reth_ethereum_primitives::EthPrimitives;
    use reth_execution_errors::BlockExecutionError;
    use reth_execution_types::BlockExecutionResult;
    use reth_primitives_traits::{NodePrimitives, RecoveredBlock};
    use revm::{
        context::{
            result::{ExecutionResult, HaltReason},
            TxEnv,
        },
        database::{BundleState, CacheDB, EmptyDB, State},
        precompile::{PrecompileId, PrecompileOutput},
    };
    use std::sync::{Arc, Mutex};

    /// A mock executor that records which precompile addresses it receives in
    /// `transact_system_txn`. Used to detect whether `WrapExecutor` correctly
    /// propagates custom precompiles registered via `apply_custom_precompiles`.
    struct TrackingExecutor {
        received_precompile_addrs: Arc<Mutex<Vec<Address>>>,
    }

    impl<DB: Database> Executor<DB> for TrackingExecutor {
        type Primitives = EthPrimitives;
        type Error = BlockExecutionError;

        fn execute_one(
            &mut self,
            _block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
        ) -> Result<
            BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>,
            Self::Error,
        > {
            unimplemented!("not used in this test")
        }

        fn execute_one_with_state_hook<F>(
            &mut self,
            _block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
            _state_hook: F,
        ) -> Result<
            BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>,
            Self::Error,
        >
        where
            F: OnStateHook + 'static,
        {
            unimplemented!("not used in this test")
        }

        fn into_state(self) -> State<DB> {
            unreachable!("not used in this test")
        }

        fn take_bundle(&mut self) -> BundleState {
            BundleState::default()
        }

        fn size_hint(&self) -> usize {
            0
        }

        fn transact_system_txn(
            &mut self,
            _evm_env: EvmEnv,
            precompiles: Vec<(Address, DynPrecompile)>,
            _tx_env: TxEnv,
        ) -> Result<ExecutionResult<HaltReason>, Self::Error> {
            // Record all precompile addresses we received.
            let mut guard = self.received_precompile_addrs.lock().unwrap();
            for (addr, _) in &precompiles {
                guard.push(*addr);
            }
            // Return a stable error so we don't need a live EVM.
            Err(BlockExecutionError::msg("mock: execution skipped"))
        }
    }

    /// Creates a minimal no-op [`DynPrecompile`] for testing.
    fn make_noop_precompile() -> DynPrecompile {
        DynPrecompile::new(PrecompileId::custom("test_noop"), |_input: PrecompileInput<'_>| {
            Ok(PrecompileOutput { gas_used: 0, bytes: Bytes::default(), reverted: false })
        })
    }

    /// The custom precompile address used across tests.
    const CUSTOM_PRECOMPILE_ADDR: Address = Address::repeat_byte(0x99);

    // -------------------------------------------------------------------------
    // Tests that demonstrate the bug
    // -------------------------------------------------------------------------

    /// `WrapExecutor::apply_custom_precompiles` is currently a no-op (see the TODO comment).
    /// This test asserts the *correct* expected behaviour — that precompiles registered via
    /// `apply_custom_precompiles` are merged into subsequent `transact_system_txn` calls —
    /// and therefore **fails** with the current code, proving the bug exists.
    #[test]
    fn wrap_executor_should_inject_custom_precompiles_into_system_txn() {
        let received = Arc::new(Mutex::new(Vec::<Address>::new()));
        let mock = TrackingExecutor { received_precompile_addrs: received.clone() };

        let mut wrap = WrapExecutor::<CacheDB<EmptyDB>, _>::new(mock);

        // Register a custom precompile at CUSTOM_PRECOMPILE_ADDR.
        wrap.apply_custom_precompiles(Arc::new(vec![(
            CUSTOM_PRECOMPILE_ADDR,
            make_noop_precompile(),
        )]));

        // Execute a system transaction passing NO explicit per-call precompiles.
        // A correct implementation would merge the custom precompiles registered above
        // into the inner executor's call.
        let _ = wrap.transact_system_txn(EvmEnv::default(), vec![], TxEnv::default());

        let guard = received.lock().unwrap();

        // EXPECTED (after fix): the custom precompile should have been forwarded.
        // ACTUAL (current bug):  WrapExecutor discards it — this assertion fails.
        assert!(
            guard.contains(&CUSTOM_PRECOMPILE_ADDR),
            "WrapExecutor::apply_custom_precompiles silently discards precompiles. \
             Expected the custom precompile at {CUSTOM_PRECOMPILE_ADDR} to be injected \
             into transact_system_txn, but the inner executor never received it.\n\
             Fix: store custom_precompiles in WrapExecutor and merge them in \
             transact_system_txn / execute_one."
        );
    }

    /// Confirms that explicit per-call precompiles passed directly to `transact_system_txn`
    /// *do* reach the inner executor.  This is the baseline that should always pass,
    /// regardless of the bug being tested above.
    #[test]
    fn wrap_executor_passes_explicit_per_call_precompiles() {
        let received = Arc::new(Mutex::new(Vec::<Address>::new()));
        let mock = TrackingExecutor { received_precompile_addrs: received.clone() };
        let mut wrap = WrapExecutor::<CacheDB<EmptyDB>, _>::new(mock);

        let explicit_addr = Address::repeat_byte(0xAB);
        let _ = wrap.transact_system_txn(
            EvmEnv::default(),
            vec![(explicit_addr, make_noop_precompile())],
            TxEnv::default(),
        );

        let guard = received.lock().unwrap();
        assert!(
            guard.contains(&explicit_addr),
            "Per-call precompiles passed directly to transact_system_txn should reach \
             the inner executor, but {explicit_addr} was not received."
        );
    }

    /// Verifies the bug through a second angle: calling `apply_custom_precompiles` and
    /// then `apply_custom_precompiles` again does not panic, but — more importantly —
    /// neither call causes the precompiles to appear when `transact_system_txn` is invoked
    /// with an empty per-call list.  The test asserts correct behaviour (both addresses
    /// present) and therefore **fails** with the buggy code.
    #[test]
    fn wrap_executor_should_accumulate_custom_precompiles_across_calls() {
        let received = Arc::new(Mutex::new(Vec::<Address>::new()));
        let mock = TrackingExecutor { received_precompile_addrs: received.clone() };
        let mut wrap = WrapExecutor::<CacheDB<EmptyDB>, _>::new(mock);

        let addr1 = Address::repeat_byte(0x01);
        let addr2 = Address::repeat_byte(0x02);

        wrap.apply_custom_precompiles(Arc::new(vec![(addr1, make_noop_precompile())]));
        wrap.apply_custom_precompiles(Arc::new(vec![(addr2, make_noop_precompile())]));

        let _ = wrap.transact_system_txn(EvmEnv::default(), vec![], TxEnv::default());

        let guard = received.lock().unwrap();
        assert!(
            guard.contains(&addr1) && guard.contains(&addr2),
            "Both custom precompiles registered via apply_custom_precompiles should be \
             forwarded, but the inner executor received only: {guard:?}.\n\
             This confirms WrapExecutor::apply_custom_precompiles is a no-op."
        );
    }
}

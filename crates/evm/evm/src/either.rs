//! Helper type that represents one of two possible executor types

use crate::{execute::Executor, OnStateHook};

// re-export Either
pub use futures_util::future::Either;
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock};

impl<'db, A, B> Executor<'db> for Either<A, B>
where
    A: Executor<'db>,
    B: Executor<'db, Primitives = A::Primitives, Error = A::Error>,
{
    type Primitives = A::Primitives;
    type Error = A::Error;

    fn execute_one(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        match self {
            Self::Left(a) => a.execute_one(block),
            Self::Right(b) => b.execute_one(block),
        }
    }

    fn execute_one_with_state_hook<F>(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
        state_hook: F,
    ) -> Result<BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    where
        F: OnStateHook + 'static,
    {
        match self {
            Self::Left(a) => a.execute_one_with_state_hook(block, state_hook),
            Self::Right(b) => b.execute_one_with_state_hook(block, state_hook),
        }
    }

    fn execute(
        self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<BlockExecutionOutput<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    {
        match self {
            Self::Left(a) => a.execute(block),
            Self::Right(b) => b.execute(block),
        }
    }

    fn execute_with_state_closure<F>(
        self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
        state: F,
    ) -> Result<BlockExecutionOutput<<Self::Primitives as NodePrimitives>::Receipt>, Self::Error>
    where
        F: FnMut(&dyn crate::state::State),
    {
        match self {
            Self::Left(a) => a.execute_with_state_closure(block, state),
            Self::Right(b) => b.execute_with_state_closure(block, state),
        }
    }

    fn into_state(self) -> Box<dyn crate::state::State + 'db> {
        match self {
            Self::Left(a) => a.into_state(),
            Self::Right(b) => b.into_state(),
        }
    }

    fn state_mut(&mut self) -> &mut dyn crate::state::State {
        match self {
            Self::Left(a) => a.state_mut(),
            Self::Right(b) => b.state_mut(),
        }
    }

    fn size_hint(&self) -> usize {
        match self {
            Self::Left(a) => a.size_hint(),
            Self::Right(b) => b.size_hint(),
        }
    }
}

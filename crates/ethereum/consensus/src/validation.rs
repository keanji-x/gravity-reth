use alloc::vec::Vec;
use alloy_consensus::{proofs::calculate_receipt_root, BlockHeader, TxReceipt};
use alloy_eips::{eip7685::Requests, Encodable2718};
use alloy_primitives::{Bloom, Bytes, B256};
use reth_chainspec::EthereumHardforks;
use reth_consensus::ConsensusError;
use reth_primitives_traits::{
    receipt::gas_spent_by_transactions, Block, BlockBody, GotExpected, Receipt, RecoveredBlock,
};

/// Validate a block with regard to execution results:
///
/// - Compares the receipts root in the block header to the block body
/// - Compares the gas used in the block header to the actual gas usage after execution
pub fn validate_block_post_execution<B, R, ChainSpec>(
    block: &RecoveredBlock<B>,
    chain_spec: &ChainSpec,
    receipts: &[R],
    requests: &Requests,
) -> Result<(), ConsensusError>
where
    B: Block,
    R: Receipt,
    ChainSpec: EthereumHardforks,
{
    // Ensure every transaction produced exactly one receipt.
    let tx_count = block.body().transactions().len();
    if tx_count != receipts.len() {
        return Err(ConsensusError::ReceiptCountMismatch { expected: tx_count, got: receipts.len() })
    }

    // Check if gas used matches the value set in header.
    let cumulative_gas_used =
        receipts.last().map(|receipt| receipt.cumulative_gas_used()).unwrap_or(0);
    if block.header().gas_used() != cumulative_gas_used {
        return Err(ConsensusError::BlockGasUsed {
            gas: GotExpected { got: cumulative_gas_used, expected: block.header().gas_used() },
            gas_spent_by_tx: gas_spent_by_transactions(receipts),
        })
    }

    // Before Byzantium, receipts contained state root that would mean that expensive
    // operation as hashing that is required for state root got calculated in every
    // transaction This was replaced with is_success flag.
    // See more about EIP here: https://eips.ethereum.org/EIPS/eip-658
    if chain_spec.is_byzantium_active_at_block(block.header().number()) &&
        let Err(error) = verify_receipts(
            block.header().receipts_root(),
            block.header().logs_bloom(),
            receipts,
        )
    {
        let receipts = receipts
            .iter()
            .map(|r| Bytes::from(r.with_bloom_ref().encoded_2718()))
            .collect::<Vec<_>>();
        tracing::debug!(%error, ?receipts, "receipts verification failed");
        return Err(error)
    }

    // Validate that the header requests hash matches the calculated requests hash
    if chain_spec.is_prague_active_at_timestamp(block.header().timestamp()) {
        let Some(header_requests_hash) = block.header().requests_hash() else {
            return Err(ConsensusError::RequestsHashMissing)
        };
        let requests_hash = requests.requests_hash();
        if requests_hash != header_requests_hash {
            return Err(ConsensusError::BodyRequestsHashDiff(
                GotExpected::new(requests_hash, header_requests_hash).into(),
            ))
        }
    }

    Ok(())
}

/// Calculate the receipts root, and compare it against the expected receipts root and logs
/// bloom.
fn verify_receipts<R: Receipt>(
    expected_receipts_root: B256,
    expected_logs_bloom: Bloom,
    receipts: &[R],
) -> Result<(), ConsensusError> {
    // Calculate receipts root.
    let receipts_with_bloom = receipts.iter().map(TxReceipt::with_bloom_ref).collect::<Vec<_>>();
    let receipts_root = calculate_receipt_root(&receipts_with_bloom);

    // Calculate header logs bloom.
    let logs_bloom = receipts_with_bloom.iter().fold(Bloom::ZERO, |bloom, r| bloom | r.bloom_ref());

    compare_receipts_root_and_logs_bloom(
        receipts_root,
        logs_bloom,
        expected_receipts_root,
        expected_logs_bloom,
    )?;

    Ok(())
}

/// Compare the calculated receipts root with the expected receipts root, also compare
/// the calculated logs bloom with the expected logs bloom.
fn compare_receipts_root_and_logs_bloom(
    calculated_receipts_root: B256,
    calculated_logs_bloom: Bloom,
    expected_receipts_root: B256,
    expected_logs_bloom: Bloom,
) -> Result<(), ConsensusError> {
    if calculated_receipts_root != expected_receipts_root {
        return Err(ConsensusError::BodyReceiptRootDiff(
            GotExpected { got: calculated_receipts_root, expected: expected_receipts_root }.into(),
        ))
    }

    if calculated_logs_bloom != expected_logs_bloom {
        return Err(ConsensusError::BodyBloomLogDiff(
            GotExpected { got: calculated_logs_bloom, expected: expected_logs_bloom }.into(),
        ))
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{b256, hex};
    use reth_ethereum_primitives::Receipt;

    // ── Issue #251 regression tests ──────────────────────────────────────────
    //
    // `validate_block_post_execution` uses
    //   `receipts.last().map(|r| r.cumulative_gas_used()).unwrap_or(0)`
    // to obtain the cumulative gas used.  When `receipts` is empty the
    // sentinel `0` is returned.  If the block header also declares
    // `gas_used = 0` the gas check passes silently even though transactions
    // are present in the block body.
    //
    // All three tests below FAIL (i.e. the assertion fires) while the bug
    // exists, and will pass once an explicit transaction/receipt count guard
    // is added.

    fn make_recovered_block_with_one_tx(
        header: alloy_consensus::Header,
    ) -> reth_primitives_traits::RecoveredBlock<reth_ethereum_primitives::Block> {
        use alloy_consensus::{BlockBody, SignableTransaction, TxLegacy};
        use alloy_primitives::{Address, Signature, U256};
        use reth_primitives_traits::RecoveredBlock;

        // Construct a signed transaction with a dummy (non-recoverable) signature.
        // We use `new_unhashed` below so actual sender recovery is never attempted.
        let tx: reth_ethereum_primitives::TransactionSigned =
            TxLegacy::default().into_signed(Signature::new(U256::ONE, U256::ONE, false)).into();

        let body: alloy_consensus::BlockBody<reth_ethereum_primitives::TransactionSigned> =
            BlockBody { transactions: vec![tx], ..Default::default() };

        let block = reth_ethereum_primitives::Block { header, body };

        // Provide a dummy sender; bypass signature recovery entirely.
        RecoveredBlock::new_unhashed(block, vec![Address::ZERO])
    }

    /// Pre-Byzantium path (block number 0 on mainnet): only the gas check runs.
    /// Empty receipts + gas_used = 0 in the header should be rejected because
    /// the block body contains a transaction.
    #[test]
    fn test_validate_block_post_execution_empty_receipts_pre_byzantium() {
        use alloy_consensus::EMPTY_ROOT_HASH;
        use alloy_eips::eip7685::Requests;
        use reth_chainspec::MAINNET;

        let header = alloy_consensus::Header {
            number: 0,
            gas_used: 0,
            receipts_root: EMPTY_ROOT_HASH,
            ..Default::default()
        };
        let recovered = make_recovered_block_with_one_tx(header);

        let receipts: Vec<Receipt> = vec![];
        let requests = Requests::default();

        // `receipts.last()` is None → sentinel 0 is used → matches gas_used = 0 in
        // header → gas check passes despite 1 unexecuted transaction.
        //
        // This assertion FAILS while the bug is present.
        let result = validate_block_post_execution(&recovered, &*MAINNET, &receipts, &requests);
        assert!(
            result.is_err(),
            "Expected error when transactions are present but receipts is empty (pre-Byzantium); \
             got Ok(())"
        );
    }

    /// Post-Byzantium path: both the gas check *and* the receipts-root /
    /// logs-bloom check run.  Setting the header's `receipts_root` to
    /// `EMPTY_ROOT_HASH` and `logs_bloom` to `Bloom::ZERO` makes
    /// `verify_receipts` also pass for an empty slice, completing the silent
    /// bypass across both checks.
    #[test]
    fn test_validate_block_post_execution_empty_receipts_post_byzantium() {
        use alloy_consensus::EMPTY_ROOT_HASH;
        use alloy_eips::eip7685::Requests;
        use alloy_primitives::Bloom;
        use reth_chainspec::MAINNET;

        // Mainnet Byzantium activates at block 4_370_000; use a block safely
        // above that threshold.
        let header = alloy_consensus::Header {
            number: 5_000_000,
            gas_limit: 30_000_000,
            gas_used: 0,
            receipts_root: EMPTY_ROOT_HASH, // matches an empty receipts slice
            logs_bloom: Bloom::ZERO,        // matches an empty receipts slice
            ..Default::default()
        };
        let recovered = make_recovered_block_with_one_tx(header);

        let receipts: Vec<Receipt> = vec![];
        let requests = Requests::default();

        // Both the gas check and the Byzantium receipts-root check pass because
        // the header fields are consistent with *zero* receipts, not one.
        //
        // This assertion FAILS while the bug is present.
        let result = validate_block_post_execution(&recovered, &*MAINNET, &receipts, &requests);
        assert!(
            result.is_err(),
            "Expected error when transactions are present but receipts is empty (post-Byzantium); \
             got Ok(())"
        );
    }

    /// Directly documents the sentinel value path that causes the bypass:
    /// `receipts.last().map(…).unwrap_or(0)` returns `0` for an empty slice.
    /// Combined with `gas_used = 0` in the header this makes the gas check a
    /// no-op, which the subsequent assertions confirm.
    #[test]
    fn test_validate_block_post_execution_gas_sentinel_bypass() {
        use alloy_consensus::EMPTY_ROOT_HASH;
        use alloy_eips::eip7685::Requests;
        use reth_chainspec::MAINNET;

        let header = alloy_consensus::Header {
            number: 0,
            gas_used: 0,
            receipts_root: EMPTY_ROOT_HASH,
            ..Default::default()
        };
        let recovered = make_recovered_block_with_one_tx(header);

        let receipts: Vec<Receipt> = vec![];

        // Show that the sentinel exactly equals gas_used, masking the missing
        // receipt.
        let sentinel = receipts.last().map(|r| r.cumulative_gas_used()).unwrap_or(0);
        assert_eq!(sentinel, 0, "sentinel for empty receipts is 0");
        assert_eq!(recovered.header().gas_used(), 0, "header declares gas_used = 0");
        assert_eq!(
            sentinel,
            recovered.header().gas_used(),
            "sentinel matches gas_used — gas check will not fire"
        );
        assert_eq!(
            recovered.body().transactions().count(),
            1,
            "block body has 1 transaction that produced no receipt"
        );

        // The function incorrectly returns Ok(()).
        // This assertion FAILS while the bug is present.
        let result =
            validate_block_post_execution(&recovered, &*MAINNET, &receipts, &Requests::default());
        assert!(
            result.is_err(),
            "Expected validation error for 1 transaction / 0 receipts; got Ok(())"
        );
    }

    #[test]
    fn test_verify_receipts_success() {
        // Create a vector of 5 default Receipt instances
        let receipts: Vec<Receipt> = vec![Receipt::default(); 5];

        // Compare against expected values
        assert!(verify_receipts(
            b256!("0x61353b4fb714dc1fccacbf7eafc4273e62f3d1eed716fe41b2a0cd2e12c63ebc"),
            Bloom::from(hex!("00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
            &receipts
        )
        .is_ok());
    }

    #[test]
    fn test_verify_receipts_incorrect_root() {
        // Generate random expected values to produce a failure
        let expected_receipts_root = B256::random();
        let expected_logs_bloom = Bloom::random();

        // Create a vector of 5 random Receipt instances
        let receipts: Vec<Receipt> = vec![Receipt::default(); 5];

        assert!(verify_receipts(expected_receipts_root, expected_logs_bloom, &receipts).is_err());
    }

    #[test]
    fn test_compare_receipts_root_and_logs_bloom_success() {
        let calculated_receipts_root = B256::random();
        let calculated_logs_bloom = Bloom::random();

        let expected_receipts_root = calculated_receipts_root;
        let expected_logs_bloom = calculated_logs_bloom;

        assert!(compare_receipts_root_and_logs_bloom(
            calculated_receipts_root,
            calculated_logs_bloom,
            expected_receipts_root,
            expected_logs_bloom
        )
        .is_ok());
    }

    #[test]
    fn test_compare_receipts_root_failure() {
        let calculated_receipts_root = B256::random();
        let calculated_logs_bloom = Bloom::random();

        let expected_receipts_root = B256::random();
        let expected_logs_bloom = calculated_logs_bloom;

        assert_eq!(
            compare_receipts_root_and_logs_bloom(
                calculated_receipts_root,
                calculated_logs_bloom,
                expected_receipts_root,
                expected_logs_bloom
            ),
            Err(ConsensusError::BodyReceiptRootDiff(
                GotExpected { got: calculated_receipts_root, expected: expected_receipts_root }
                    .into()
            ))
        );
    }

    #[test]
    fn test_compare_log_bloom_failure() {
        let calculated_receipts_root = B256::random();
        let calculated_logs_bloom = Bloom::random();

        let expected_receipts_root = calculated_receipts_root;
        let expected_logs_bloom = Bloom::random();

        assert_eq!(
            compare_receipts_root_and_logs_bloom(
                calculated_receipts_root,
                calculated_logs_bloom,
                expected_receipts_root,
                expected_logs_bloom
            ),
            Err(ConsensusError::BodyBloomLogDiff(
                GotExpected { got: calculated_logs_bloom, expected: expected_logs_bloom }.into()
            ))
        );
    }
}

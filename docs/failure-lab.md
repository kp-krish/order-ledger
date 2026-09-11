# Failure lab

The integration suite contains both the failures and their fixes.

## Out-of-order lifecycle

`lowerSequenceIsRecordedAsStaleAndDoesNotMutateState` sends sequence `1`, then `3`,
then `2`. Without a durable order watermark, last-write-wins logic would regress a
shipped order to cancelled and release stock that already shipped. The test proves
the sequence-2 event is retained for audit with outcome `STALE` while inventory and
the order projection remain shipped.

Run the same scenario against the local three-partition topic:

```shell
python scripts/producer.py --partitions 3 --orders 10 --out-of-order 10
```

## Deadlock

`naiveOppositeRowLockingActuallyDeadlocks` opens two real PostgreSQL transactions.
One locks `SKU-001` then requests `SKU-002`; the other does the reverse. PostgreSQL
detects the cycle and aborts exactly one transaction.

`oppositeInputOrdersCompleteWithConsistentSkuLocking` then submits 20 orders whose
payloads alternate those opposite SKU orders. Production processing sorts the SKU
set before `SELECT ... FOR UPDATE`, so all transactions acquire `SKU-001` before
`SKU-002`; every transaction completes and the final quantities are asserted.


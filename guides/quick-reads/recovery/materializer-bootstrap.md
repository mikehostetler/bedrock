# Materializer Bootstrap

**Get the shard layout back from a valid cache, or rebuild the cache from recovered history.**

Recovery needs to know the cluster's shard boundaries before it can place
resolvers or route anything. Those boundaries live in the committed keyspace
under `\xff/system/shard_keys/`, which means recovery has to bring one
materializer — the one serving the system shard, tag 0 — back online and read
them out of it.

Materializers are caches. The recovered logs and object-storage chunks hold
the durable history. The phase can reuse a valid cache, or create a new cache
and replay that history.

## Fresh Cluster

With no prior logs there is nothing to recover, so the phase invents the
starting layout: two shards, the system shard (tag 0) covering `0xFF` to the
end of the keyspace, and the user shard (tag 1) covering everything below
`0xFF`.

## Existing Cluster

1. **Check the named system materializers** from the prior
   [`CoreState`](../transaction-system-layout.md). These names are cache
   hints, not durable authority.
2. **Lock and replay a named cache** through the recovery version.
3. **Read its committed membership** and reuse it only if that membership
   still assigns tag 0 to the named worker.
4. **Rebuild when no named cache is valid.** Create a tag-0 materializer,
   replay the recovered logs and chunks, and wait for catchup.
5. **Read the shard layout** from `\xff/system/shard_keys/*` and the member
   set from `\xff/system/materializers/*`.

The distributor manages data-tag materializers after recovery. This phase
only restores the tag-0 cache that recovery needs to read system metadata.

## Stalls

The phase stalls and recovery retries if reconstruction fails, catchup times
out, or the recovered layout reads empty. An empty read is treated as a
failure rather than as "this cluster has no shards", because the difference
matters: see
[The System Keyspace](../system-keyspace.md).

## Next Phase

Recovery proceeds to [commit proxy startup](proxy-startup.md) with the system
materializer's pid and the shard layout in hand.

---

**Implementation**: `lib/bedrock/control_plane/director/recovery/system_shard_bootstrap_phase.ex`

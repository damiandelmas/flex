---
name: flex:ledger
description: Use Flex Ledger as a SQL-native continuity and commentary layer over exact objects in any Flex cell. Use when the user asks to annotate the current conversation or another Flex object, recover session context after compaction, inspect an annotation trajectory, or revise/remove commentary without changing its canonical target.
---

# flex:ledger

Ledger is a thin authored index over canonical Flex objects. Canonical content
and authority remain with each target's owning cell.

```text
Flex cell + object ID   canonical evidence
Ledger annotation      why that object matters
SQL                     selection and composition
@index                  navigable continuity map
@hydrate                compact lineage plus recent exact evidence
```

Ledger's agent interface is its presets and ordinary SQL. MCP, Console, and
the raw Flex search shell are transports for the same relational contract.

## Begin with the live contract

For the calling session:

```text
cell="ledger" query="@orient"
```

The runtime-bound session is the default seed. This orientation is
content-free: it describes the selected topology, available relations,
navigation surfaces, mutation SQL, and limitations without returning
annotations or messages.

Whole-cell investigation uses:

```text
cell="ledger" query="@orient global"
```

The live schema and presets are authoritative. The examples here illustrate
the procedure; orientation supplies the executable contract.

## Recover continuity

After compaction, begin directly with:

```text
cell="ledger" query="@hydrate"
```

`@hydrate` is the continuity handoff. It supersedes the general Flex habit of
orienting a target provider cell before reading it: do not run `@orient` on
Codex, Claude Code, or another target merely to reconstruct the recovered
lineage. Ledger returns the selected annotations together with their exact
canonical target messages in provider-native source order.

Large handoffs may be delivered through cursor or result-window pages. Follow
the returned continuation automatically as part of this one hydration
operation, until every selected landmark's annotation and canonical source
body have been read. Pagination is transport mechanics, not a new research
step. The terminal receipt reports:

```text
lineage_complete
remaining_landmarks
has_more
next
```

`lineage_complete=1` means every selected landmark is represented. Complete
hydration is established only after the cursor/window sequence has delivered
every selected canonical source body. Do not substitute a fresh target-cell
search for those continuations.

Use explicit cursor mode only when the initial `@hydrate` receipt says more
landmarks remain outside its first packet. Follow each returned `next` query
until `has_more=0`:

```text
cell="ledger" query="@hydrate after=__start__"
cell="ledger" query="<NEXT>"
```

If the general Flex result gate windows a large response, repeat the identical
query with `!` to receive only unseen fragments from its immutable,
caller-scoped snapshot. Hydration is complete when that window reports
`has_more=0`.

Only after hydration is complete may an agent orient or query a target cell,
and only when the next task creates a genuinely new corpus question that the
recovered lineage does not answer. That is a deliberate investigation phase,
not continuity recovery.

Native conversation compaction is supplemental context. Ledger rehydration
begins with `@hydrate`.

## Navigate before opening everything

```text
cell="ledger" query="@index"
cell="ledger" query="@index seed=<ANNOTATION_ID> depth=1 limit=40"
```

`@index` is a query-local PageIndex-style projection: identity, map, seeded
node, lineage, neighborhood, and relations. Its nodes resolve to Ledger
annotations and their canonical targets. Use it when the shape of the
trajectory matters; ordinary post-compaction continuity normally needs only
`@hydrate`.

## Annotate the current conversation

When the user asks to annotate this conversation, the runtime supplies its
exact provider and session identity.

1. Run caller-scoped `@orient` to resolve the target cell and session seed.
2. Open the target provider's compact message index for that exact session:

   ```text
   cell="<TARGET_CELL>" query="@message-index session=<SESSION_ID> limit=200"
   ```

3. Select only meaningful completed user or assistant turns.
4. Execute the `INSERT` shown in Ledger's `mutation_contract` against the
   public `annotations` relation.
5. Verify the result through `@index` or direct SQL.

Completed provider messages are structurally addressable immediately. Their
text, metadata, FTS entries, and relations publish together; vector embeddings
converge asynchronously. Annotate the intended exact message.

## Mutate through SQL

The global and caller-scoped orientations expose complete `add`, `revise`, and
`remove` statements. Their essential forms are:

```sql
INSERT INTO annotations(
    annotation_id, note, target_cell_id, target_chunk_id,
    wing, hall, room, weight,
    author_provider, author_session_id, author_source
) VALUES (
    ledger_annotation_id(:target_cell_id, :target_chunk_id),
    :note, :target_cell_id, :target_chunk_id,
    :wing, :hall, :room, :weight,
    ledger_author_provider(),
    ledger_author_session_id(),
    ledger_author_source()
)
RETURNING annotation_id, target_cell_id, target_chunk_id;
```

```sql
UPDATE annotations
SET note=:note,
    wing=:wing,
    hall=:hall,
    room=:room,
    weight=:weight,
    author_provider=ledger_author_provider(),
    author_session_id=ledger_author_session_id(),
    author_source=ledger_author_source()
WHERE target_cell_id=:target_cell_id
  AND target_chunk_id=:target_chunk_id
RETURNING annotation_id;
```

```sql
DELETE FROM annotations
WHERE target_cell_id=:target_cell_id
  AND target_chunk_id=:target_chunk_id
RETURNING annotation_id, target_cell_id, target_chunk_id;
```

Use bound parameters when the transport supports them; otherwise quote SQL
literals correctly. Ledger preserves prior versions, authorship, and FTS
synchronization transactionally. Mutations affect Ledger while canonical target
content remains in its owning cell.

## Query and compose

Everything beyond the presets is ordinary SQL:

```sql
SELECT *
FROM annotations
WHERE target_cell_id=:cell_id
ORDER BY updated_at DESC;
```

```sql
SELECT a.*, k.rank
FROM keyword(:term) k
JOIN annotations a ON a.annotation_id=k.id
ORDER BY k.rank DESC;
```

```sql
SELECT *
FROM annotation_history
WHERE annotation_id=:annotation_id
ORDER BY revision;
```

Use Flex Meta to attach registered target cells read-only and join exact target
coordinates to their canonical objects. Expand the seed with `self('SELECT
...')` when predecessor sessions or other Flex objects should participate in
the same temporary query world. SQL expresses further selection and
composition directly.

## Annotation discipline

Annotate decisions, discoveries, corrections, durable preferences, verified
milestones, and unresolved boundaries that materially help later work. Sparse
landmarks are more useful than progress narration or copied operational reports.

Write compact observational notes anchored in what the target supports:

- State verified facts crisply.
- Distinguish observations from interpretations.
- Keep hypotheses provisional.
- Preserve meaningful disagreement and uncertainty.
- Prefer exact source turns or artifacts over opaque tool envelopes.
- Revise the existing annotation when interpretation changes so one target
  retains one evolving note and its preserved history.

The annotation is navigation and interpretation. The recovered target remains
the evidence.

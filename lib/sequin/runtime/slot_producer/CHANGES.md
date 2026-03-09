# Legacy Inheritance Support

This document explains the changes made to add support for PostgreSQL legacy table inheritance in CDC event emission.

## Background

PostgreSQL supports two forms of table hierarchy:

- **Declarative partitioning** (`relispartition = true`): a child table is a physical partition of a partitioned parent. Each partition has exactly one parent.
- **Legacy inheritance** (`relispartition = false`, entries in `pg_inherits`): a child table inherits columns from one or more parent tables. A child can have multiple parents and the hierarchy can be arbitrarily deep.

The previous code used `pg_inherits` without checking `relispartition`, so it could not distinguish between the two cases. It also pattern-matched on a single-row result, which crashed at runtime for any legacy-inherited table with more than one parent.

The desired behaviour for legacy inheritance: emit the same CDC record once under the child table's identity **and** once for every ancestor (parent, grandparent, …) in the hierarchy.

---

## Changes to `relation.ex`

### 1. New `ancestor_relations` field on the struct

```elixir
field :ancestor_relations, list(t()), default: []
```

Every `Relation` now carries a list of "virtual" relations — one per ancestor table in the legacy inheritance chain. For plain tables, partitions, and the VirtualBackend, this list is empty and the behaviour is unchanged. For a legacy-inherited child, it will hold one `Relation` per ancestor (parent, grandparent, …).

---

### 2. `ancestor_relations: []` in VirtualBackend clause

```elixir
%__MODULE__{
  ...
  ancestor_relations: []
}
```

The VirtualBackend path skips all DB queries (used for benchmarking/tests). It always produces a plain, self-contained relation, so the new field is just initialised to the empty default.

---

### 3. Replaced `partition_query` with `relation_type_query`

**Old query** used an `INNER JOIN pg_inherits` and a hard-coded single-row pattern match `[[parent_id, parent_schema, parent_name]]`. This crashed at runtime whenever a table had more than one parent (which can only happen in legacy inheritance, because native partitions always have exactly one parent).

**New query** uses a `LEFT JOIN` and also selects `c.relispartition`:

```sql
SELECT
  c.relispartition,
  p.inhparent as parent_id,
  pn.nspname as parent_schema,
  pc.relname as parent_name
FROM pg_class c
LEFT JOIN pg_inherits p ON p.inhrelid = c.oid
LEFT JOIN pg_class pc ON pc.oid = p.inhparent
LEFT JOIN pg_namespace pn ON pn.oid = pc.relnamespace
WHERE c.oid = $1;
```

`pg_class.relispartition` is the key field. Postgres sets it to `true` only for tables that are children of a declarative partitioned table, and `false` for everything else — including legacy-inherited children. This lets us distinguish the two cases unambiguously.

The result is matched in three branches:

| Result | Meaning | Action |
|--------|---------|--------|
| `[[_, nil, nil, nil]]` | No parents at all | Plain table — use child's own identity |
| `[[true, parent_id, …]]` | `relispartition = true` | Native partition — use parent's identity (existing behaviour) |
| anything else | `relispartition = false` with ≥ 1 parent rows | Legacy inheritance — keep child's identity, fetch all ancestors |

---

### 4. `fetch_all_ancestors/2`

Called only in the legacy-inheritance branch. It runs a recursive CTE:

```sql
WITH RECURSIVE ancestors AS (
  -- base case: direct parents of the given table
  SELECT p.inhparent AS ancestor_id, ...
  FROM pg_inherits p ...
  WHERE p.inhrelid = $1

  UNION

  -- recursive case: parents of those parents, but only for non-partition tables
  SELECT p2.inhparent AS ancestor_id, ...
  FROM ancestors a
  JOIN pg_class child_c ON child_c.oid = a.ancestor_id
  ...
  WHERE child_c.relispartition = false
)
SELECT DISTINCT ancestor_id, ancestor_schema, ancestor_name FROM ancestors;
```

- The base case collects **direct parents** of the child table.
- The recursive case walks **up the hierarchy** collecting grandparents, great-grandparents, etc.
- `WHERE child_c.relispartition = false` stops the traversal if it ever reaches a native partition boundary (a safe guard — in practice, a legacy-inherited table's parents are always regular tables).
- `DISTINCT` de-duplicates in case of diamond inheritance (a table with two parents that share a common grandparent).

The function returns a plain list of `%{id, schema, name}` maps — one per unique ancestor.

---

### 5. `built_ancestor_relations` and updated final struct

After the attnum/PK enrichment is done for the child, ancestor `Relation` structs are created:

```elixir
built_ancestor_relations =
  Enum.map(ancestor_relations, fn ancestor ->
    %__MODULE__{
      id: ancestor.id,
      columns: enriched_columns,   # same WAL column layout as the child
      schema: ancestor.schema,
      table: ancestor.name,
      parent_table_id: ancestor.id,
      ancestor_relations: []
    }
  end)
```

Each ancestor relation is a full `%Relation{}` struct, but with the **ancestor's own OID, schema, and table name**. The `columns` are reused from the child because the WAL tuple data is always emitted by Postgres using the child table's column layout — the ancestor relations are "virtual" identities for the same physical row.

`parent_table_id` is set to the ancestor's own OID (not the child's), because that is what consumers use as `table_oid` to match subscription filters.

---

## Changes to `processor.ex`

### 6. `Enum.map` → `Enum.flat_map` in `handle_events`

```elixir
|> Enum.flat_map(fn %Message{} = msg ->
  case Decoder.decode_message(msg.payload) do
    %LogicalMessage{} = payload ->
      [%{msg | message: payload}]          # always one message

    %type{} = payload when type in [Insert, Update, Delete] ->
      cast_messages(payload, msg, state.relations)  # one or more messages
  end
end)
```

Previously `Enum.map` produced exactly one output per WAL event. Using `flat_map` allows `cast_messages` to return a **list** — one envelope for the child table plus one per ancestor — and have them all emitted downstream as individual events. For non-inherited tables the list will have exactly one element, so the behaviour is unchanged.

`LogicalMessage` is handled inline because it doesn't involve relations at all and always produces exactly one output.

---

### 7. `cast_message` → `cast_messages`, returning a list

The three old `cast_message` functions for Insert, Update, and Delete each returned a single `%SlotProcessor.Message{}`. They are replaced by `cast_messages` functions that return a list:

```elixir
all_relations = [relation | relation.ancestor_relations]

Enum.with_index(all_relations)
|> Enum.map(fn {rel, idx} ->
  inner = %SlotProcessor.Message{
    table_schema: rel.schema,
    table_name:   rel.table,
    table_oid:    rel.parent_table_id,
    ...
    idempotency_key: idempotency_key(envelope, idx, rel.parent_table_id)
  }
  %{envelope | message: inner}
end)
```

For each relation in `all_relations` a full `SlotProducer.Message` envelope is produced, wrapping a `SlotProcessor.Message` with that relation's table identity. The field values (`ids`, `fields`, `old_fields`) are extracted from the same WAL tuple data every time — only the table metadata differs between the copies.

---

### 8. `idempotency_key/3` helper

```elixir
defp idempotency_key(envelope, 0, _table_oid) do
  Base.encode64("#{envelope.commit_lsn}:#{envelope.commit_idx}")
end

defp idempotency_key(envelope, _idx, table_oid) do
  Base.encode64("#{envelope.commit_lsn}:#{envelope.commit_idx}:#{table_oid}")
end
```

When `idx == 0` (the child itself), the key is identical to the old format — backward-compatible for non-inherited tables and for existing consumers subscribed to the child. For ancestor messages (`idx > 0`), the ancestor's OID is appended to make the key globally unique, so each copy of the event can be safely deduplicated independently by downstream consumers.

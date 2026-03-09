defmodule Sequin.Runtime.SlotProducer.Relation do
  @moduledoc """
  Represents a relation in the database.
  """
  use TypedStruct

  alias Sequin.Databases.DatabaseUpdateWorker
  alias Sequin.Postgres
  alias Sequin.Postgres.VirtualBackend
  alias Sequin.Runtime.PostgresAdapter.Decoder
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Relation, as: RawRelation
  alias Sequin.Runtime.SlotProducer.PostgresRelationHashCache

  require Logger

  typedstruct do
    field :id, non_neg_integer()
    field :columns, list(map())
    field :schema, String.t()
    field :table, String.t()
    field :parent_table_id, integer()
    field :ancestor_relations, list(t()), default: []
  end

  defmodule Column do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :attnum, non_neg_integer()
      field :type_modifier, non_neg_integer()
      field :pk?, boolean()
      field :type, String.t()
      field :name, String.t()
      field :flags, list(String.t())
    end
  end

  # Map of common Postgres type OIDs to type names
  # Only includes types used by VirtualBackend/BenchmarkSource
  @oid_to_type_name %{
    16 => "bool",
    17 => "bytea",
    20 => "int8",
    21 => "int2",
    23 => "int4",
    25 => "text",
    700 => "float4",
    701 => "float8",
    1082 => "date",
    1083 => "time",
    1114 => "timestamp",
    1184 => "timestamptz",
    2950 => "uuid"
  }

  @spec parse_relation(module(), binary(), db_id :: String.t(), (-> Postgres.db_conn())) :: t()
  def parse_relation(VirtualBackend, msg, _db_id, _conn_fn) do
    # For virtual backends (benchmarking/testing), skip DB queries
    %RawRelation{id: id, columns: columns, namespace: schema, name: table} = Decoder.decode_message(msg)

    columns =
      columns
      |> Enum.with_index(1)
      |> Enum.map(fn {%RawRelation.Column{} = col, idx} ->
        # Convert OID to type name string for ValueCaster compatibility
        type_name = Map.get(@oid_to_type_name, col.type, "text")

        %Column{
          attnum: col.attnum || idx,
          type_modifier: col.type_modifier,
          pk?: col.pk? || :key in col.flags,
          type: type_name,
          name: col.name,
          flags: col.flags
        }
      end)

    %__MODULE__{
      id: id,
      columns: columns,
      schema: schema,
      table: table,
      parent_table_id: id,
      ancestor_relations: []
    }
  end

  def parse_relation(_backend_mod, msg, db_id, conn_fn) do
    conn = conn_fn.()
    %RawRelation{id: id, columns: columns, namespace: schema, name: table} = raw_relation = Decoder.decode_message(msg)

    columns =
      Enum.map(columns, fn %RawRelation.Column{} = col ->
        %Column{
          attnum: col.attnum,
          type_modifier: col.type_modifier,
          pk?: col.pk?,
          type: col.type,
          name: col.name,
          flags: col.flags
        }
      end)

    # Determine if this table is a native partition, uses legacy inheritance, or is a plain table.
    # pg_class.relispartition distinguishes native partitions (true) from legacy inheritance (false).
    relation_type_query = """
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
    """

    {parent_info, ancestor_relations} =
      case Postgres.query(conn, relation_type_query, [id]) do
        # Plain table: no parents at all
        {:ok, %{rows: [[_relispartition, nil, nil, nil]]}} ->
          {%{id: id, schema: schema, name: table}, []}

        # Native partition: relispartition = true, single parent
        {:ok, %{rows: [[true, parent_id, parent_schema, parent_name]]}} ->
          {%{id: parent_id, schema: parent_schema, name: parent_name}, []}

        # Legacy inheritance: relispartition = false with one or more parents
        {:ok, %{rows: _rows}} ->
          ancestors = fetch_all_ancestors(conn, id)
          {%{id: id, schema: schema, name: table}, ancestors}
      end

    # Get attnums for the actual table
    attnum_query = """
    with pk_columns as (
      select a.attname
      from pg_index i
      join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
      where i.indrelid = $1
      and i.indisprimary
    ),
    column_info as (
      select a.attname, a.attnum,
             (select typname
              from pg_type
              where oid = case when t.typtype = 'd'
                              then t.typbasetype
                              else t.oid
                         end) as base_type
      from pg_attribute a
      join pg_type t on t.oid = a.atttypid
      where a.attrelid = $1
      and a.attnum > 0
      and not a.attisdropped
    )
    select c.attname, c.attnum, c.base_type, (pk.attname is not null) as is_pk
    from column_info c
    left join pk_columns pk on pk.attname = c.attname;
    """

    {:ok, %{rows: attnum_rows}} = Postgres.query(conn, attnum_query, [parent_info.id])

    # Enrich columns with primary key information and attnums
    enriched_columns =
      Enum.map(columns, fn %{name: name} = column ->
        case Enum.find(attnum_rows, fn [col_name, _, _, _] -> col_name == name end) do
          [_, attnum, base_type, is_pk] ->
            %{column | pk?: is_pk, attnum: attnum, type: base_type}

          nil ->
            column
        end
      end)

    # Create a relation with enriched columns
    enriched_relation = %{raw_relation | columns: enriched_columns}

    # Compare schema hashes to detect changes
    current_hash = PostgresRelationHashCache.compute_schema_hash(enriched_relation)
    stored_hash = PostgresRelationHashCache.get_schema_hash(db_id, id)

    if stored_hash != current_hash do
      Logger.info("[SlotProcessorServer] Schema changes detected for table, enqueueing database update",
        relation_id: id,
        schema: parent_info.schema,
        table: parent_info.name
      )

      PostgresRelationHashCache.update_schema_hash(db_id, id, current_hash)
      DatabaseUpdateWorker.enqueue(db_id, unique_period: 0)
    end

    # Build ancestor Relation structs for legacy inheritance.
    # Each ancestor uses the same enriched columns as the child (same WAL tuple layout),
    # but carries the ancestor's schema, table name, and OID.
    built_ancestor_relations =
      Enum.map(ancestor_relations, fn ancestor ->
        %__MODULE__{
          id: ancestor.id,
          columns: enriched_columns,
          schema: ancestor.schema,
          table: ancestor.name,
          parent_table_id: ancestor.id,
          ancestor_relations: []
        }
      end)

    %__MODULE__{
      id: id,
      columns: enriched_columns,
      schema: parent_info.schema,
      table: parent_info.name,
      parent_table_id: parent_info.id,
      ancestor_relations: built_ancestor_relations
    }
  end

  # Recursively fetches all ancestors (parents, grandparents, etc.) for a legacy-inherited table.
  # Stops traversal at any native partition boundary (relispartition = true).
  defp fetch_all_ancestors(conn, table_id) do
    recursive_ancestor_query = """
    WITH RECURSIVE ancestors AS (
      SELECT
        p.inhparent AS ancestor_id,
        pc.relname AS ancestor_name,
        pn.nspname AS ancestor_schema
      FROM pg_inherits p
      JOIN pg_class pc ON pc.oid = p.inhparent
      JOIN pg_namespace pn ON pn.oid = pc.relnamespace
      WHERE p.inhrelid = $1

      UNION

      SELECT
        p2.inhparent AS ancestor_id,
        pc2.relname AS ancestor_name,
        pn2.nspname AS ancestor_schema
      FROM ancestors a
      JOIN pg_class child_c ON child_c.oid = a.ancestor_id
      JOIN pg_inherits p2 ON p2.inhrelid = a.ancestor_id
      JOIN pg_class pc2 ON pc2.oid = p2.inhparent
      JOIN pg_namespace pn2 ON pn2.oid = pc2.relnamespace
      WHERE child_c.relispartition = false
    )
    SELECT DISTINCT ancestor_id, ancestor_schema, ancestor_name
    FROM ancestors;
    """

    {:ok, %{rows: rows}} = Postgres.query(conn, recursive_ancestor_query, [table_id])

    Enum.map(rows, fn [ancestor_id, ancestor_schema, ancestor_name] ->
      %{id: ancestor_id, schema: ancestor_schema, name: ancestor_name}
    end)
  end
end

defmodule Sequin.Runtime.SlotProducer.Processor do
  @moduledoc """
  A simple GenStage consumer with manual demand for testing sync_info behavior.
  """

  @behaviour Sequin.Runtime.SlotProducer.ProcessorBehaviour

  use GenStage

  alias Sequin.Error.ServiceError
  alias Sequin.Postgres.ValueCaster
  alias Sequin.Runtime.PostgresAdapter.Decoder
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Delete
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Insert
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.LogicalMessage
  alias Sequin.Runtime.PostgresAdapter.Decoder.Messages.Update
  alias Sequin.Runtime.SlotProcessor
  alias Sequin.Runtime.SlotProcessor.Message.Field
  alias Sequin.Runtime.SlotProducer.BatchMarker
  alias Sequin.Runtime.SlotProducer.Message
  alias Sequin.Runtime.SlotProducer.ProcessorBehaviour
  alias Sequin.Runtime.SlotProducer.Relation
  alias Sequin.Runtime.SlotProducer.ReorderBuffer

  require Logger

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def config(key) do
    Keyword.fetch!(config(), key)
  end

  def config do
    Application.fetch_env!(:sequin, __MODULE__)
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Runtime.SlotProducer.Relation

    typedstruct do
      field :relations, %{required(table_oid :: String.t()) => Relation.t()}, default: %{}
      field :id, String.t()
      field :partition_idx, non_neg_integer()
      field :reorder_buffer, pid() | nil
    end
  end

  def partition_count do
    System.schedulers_online()
  end

  def partitions do
    0..(partition_count() - 1)
  end

  def start_link(opts \\ []) do
    id = Keyword.fetch!(opts, :id)
    partition_idx = Keyword.fetch!(opts, :partition_idx)
    GenStage.start_link(__MODULE__, opts, name: via_tuple(id, partition_idx))
  end

  def via_tuple(id, partition_idx) do
    {:via, :syn, {:replication, {__MODULE__, id, partition_idx}}}
  end

  @impl ProcessorBehaviour
  def handle_relation(pid, relation) when is_pid(pid) do
    GenStage.cast(pid, {:relation, relation})
  end

  def handle_relation(id, relation) do
    Enum.each(partitions(), fn partition_idx ->
      GenStage.cast(via_tuple(id, partition_idx), {:relation, relation})
    end)
  end

  @impl ProcessorBehaviour
  def handle_batch_marker(pid, batch) when is_pid(pid) do
    GenStage.async_info(pid, {:batch_marker, batch})
  end

  def handle_batch_marker(id, batch) do
    Enum.each(partitions(), fn partition_idx ->
      GenStage.async_info(via_tuple(id, partition_idx), {:batch_marker, batch})
    end)
  end

  @impl GenStage
  def init(opts) do
    id = Keyword.fetch!(opts, :id)
    account_id = Keyword.fetch!(opts, :account_id)
    partition_idx = Keyword.fetch!(opts, :partition_idx)

    Logger.metadata(replication_id: id, account_id: account_id)

    Sequin.name_process({__MODULE__, {id, partition_idx}})

    state = %State{
      id: id,
      partition_idx: partition_idx
    }

    # Get subscription configuration - default to empty for manual subscriptions
    {:producer_consumer, state, subscribe_to: Keyword.get(opts, :subscribe_to, []), buffer_size: :infinity}
  end

  @impl GenStage
  def handle_subscribe(:consumer, _opts, {pid, _ref}, state) do
    # ReorderBuffer subscribing to us
    {:automatic, %{state | reorder_buffer: pid}}
  end

  def handle_subscribe(:producer, _opts, _producer, state) do
    # We're subscribing to SlotProducer - automatic subscription
    {:automatic, state}
  end

  @impl GenStage
  def handle_events(events, _from, state) do
    messages =
      events
      |> Enum.flat_map(fn %Message{} = msg ->
        case Decoder.decode_message(msg.payload) do
          %LogicalMessage{} = payload ->
            [%{msg | message: payload}]

          %type{} = payload when type in [Insert, Update, Delete] ->
            cast_messages(payload, msg, state.relations)
        end
      end)
      |> Enum.reject(&is_nil/1)
      |> Enum.reject(&internal_change?/1)

    {:noreply, messages, state}
  end

  @impl GenStage
  def handle_cast({:relation, %Relation{} = relation}, state) do
    {:noreply, [], %{state | relations: Map.put(state.relations, relation.id, relation)}}
  end

  @impl GenStage
  def handle_info({:batch_marker, %BatchMarker{} = marker}, state) do
    if is_nil(state.reorder_buffer) do
      raise "Received batch marker without a reorder buffer"
    end

    ReorderBuffer.handle_batch_marker(state.id, %{marker | producer_partition_idx: state.partition_idx})

    {:noreply, [], state}
  end

  # Returns a list of SlotProducer.Message envelopes — one for the child relation itself,
  # plus one for each ancestor relation (for legacy-inherited tables).
  @spec cast_messages(decoded_message :: map(), envelope :: Message.t(), schemas :: map()) :: [Message.t()]
  defp cast_messages(%Insert{} = payload, %Message{} = envelope, schemas) do
    relation = Map.get(schemas, payload.relation_id)
    all_relations = [relation | relation.ancestor_relations]

    Enum.with_index(all_relations)
    |> Enum.map(fn {rel, idx} ->
      ids = data_tuple_to_ids(rel.columns, payload.tuple_data)

      inner = %SlotProcessor.Message{
        action: :insert,
        errors: nil,
        ids: ids,
        table_schema: rel.schema,
        table_name: rel.table,
        table_oid: rel.parent_table_id,
        fields: data_tuple_to_fields(ids, rel.columns, payload.tuple_data),
        trace_id: UUID.uuid4(),
        commit_lsn: envelope.commit_lsn,
        commit_idx: envelope.commit_idx,
        commit_timestamp: envelope.commit_ts,
        transaction_annotations: envelope.transaction_annotations,
        byte_size: envelope.byte_size,
        batch_idx: envelope.batch_idx,
        idempotency_key: idempotency_key(envelope, idx, rel.parent_table_id)
      }

      %{envelope | message: inner}
    end)
  end

  defp cast_messages(%Update{} = payload, %Message{} = envelope, schemas) do
    relation = Map.get(schemas, payload.relation_id)
    all_relations = [relation | relation.ancestor_relations]

    Enum.with_index(all_relations)
    |> Enum.map(fn {rel, idx} ->
      ids = data_tuple_to_ids(rel.columns, payload.tuple_data)

      old_fields =
        if payload.old_tuple_data do
          data_tuple_to_fields(ids, rel.columns, payload.old_tuple_data)
        end

      inner = %SlotProcessor.Message{
        action: :update,
        errors: nil,
        ids: ids,
        table_schema: rel.schema,
        table_name: rel.table,
        table_oid: rel.parent_table_id,
        old_fields: old_fields,
        fields: data_tuple_to_fields(ids, rel.columns, payload.tuple_data),
        trace_id: UUID.uuid4(),
        commit_lsn: envelope.commit_lsn,
        commit_idx: envelope.commit_idx,
        commit_timestamp: envelope.commit_ts,
        transaction_annotations: envelope.transaction_annotations,
        byte_size: envelope.byte_size,
        batch_idx: envelope.batch_idx,
        idempotency_key: idempotency_key(envelope, idx, rel.parent_table_id)
      }

      %{envelope | message: inner}
    end)
  end

  defp cast_messages(%Delete{} = payload, %Message{} = envelope, schemas) do
    relation = Map.get(schemas, payload.relation_id)
    all_relations = [relation | relation.ancestor_relations]

    prev_tuple_data =
      if payload.old_tuple_data do
        payload.old_tuple_data
      else
        payload.changed_key_tuple_data
      end

    Enum.with_index(all_relations)
    |> Enum.map(fn {rel, idx} ->
      ids = data_tuple_to_ids(rel.columns, prev_tuple_data)

      inner = %SlotProcessor.Message{
        action: :delete,
        errors: nil,
        ids: ids,
        table_schema: rel.schema,
        table_name: rel.table,
        table_oid: rel.parent_table_id,
        old_fields: data_tuple_to_fields(ids, rel.columns, prev_tuple_data),
        trace_id: UUID.uuid4(),
        commit_lsn: envelope.commit_lsn,
        commit_idx: envelope.commit_idx,
        commit_timestamp: envelope.commit_ts,
        transaction_annotations: envelope.transaction_annotations,
        byte_size: envelope.byte_size,
        batch_idx: envelope.batch_idx,
        idempotency_key: idempotency_key(envelope, idx, rel.parent_table_id)
      }

      %{envelope | message: inner}
    end)
  end

  # For the child relation (idx == 0), preserves the existing key format.
  # For ancestor relations (idx > 0), appends the ancestor OID to ensure uniqueness.
  defp idempotency_key(envelope, 0, _table_oid) do
    Base.encode64("#{envelope.commit_lsn}:#{envelope.commit_idx}")
  end

  defp idempotency_key(envelope, _idx, table_oid) do
    Base.encode64("#{envelope.commit_lsn}:#{envelope.commit_idx}:#{table_oid}")
  end

  defp internal_change?(%struct{} = msg) when struct in [Insert, Update, Delete] do
    msg.table_schema in [@config_schema, @stream_schema] and msg.table_schema != "public"
  end

  defp internal_change?(_msg), do: false

  def data_tuple_to_ids(columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.filter(fn {col, _} -> col.pk? end)
    # Very important - the system expects the PKs to be sorted by attnum
    |> Enum.sort_by(fn {col, _} -> col.attnum end)
    |> Enum.map(fn {_, value} -> value end)
  end

  @spec data_tuple_to_fields(list(), [map()], tuple()) :: [Field.t()]
  def data_tuple_to_fields(id_list, columns, tuple_data) do
    columns
    |> Enum.zip(Tuple.to_list(tuple_data))
    |> Enum.map(fn {%{name: name, attnum: attnum, type: type}, value} ->
      case ValueCaster.cast(type, value) do
        {:ok, casted_value} ->
          %Field{
            column_name: name,
            column_attnum: attnum,
            value: casted_value
          }

        {:error, %ServiceError{code: :invalid_json} = error} ->
          details = Map.put(error.details, :ids, id_list)

          raise %{
            error
            | details: details,
              message: error.message <> " for column `#{name}` in row with ids: #{inspect(id_list)}"
          }
      end
    end)
  end
end

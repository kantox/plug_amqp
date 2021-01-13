defmodule Plug.AMQP.Consumer do
  @moduledoc ~s"""
  A consumer for *AMQP* protocol.

  This module provides a consumer for managing asynchronous remote procedure
  calls that expect no response.

  ## Usage

  To use this consumer we must provide a `t:message_handler/0` function. The
  consumer will call this function to manage requests and acknowledge messages.
  """
  use GenServer

  alias AMQP.{Basic, Channel, Connection}
  alias __MODULE__, as: State

  require Logger

  @typep t() :: %State{
           backoff: Backoff.t(),
           chan: Channel.t() | nil,
           chan_ref: reference() | nil,
           conn: Connection.t() | nil,
           index: %{reference() => {Task.t(), keyword()}},
           opts: options()
         }

  defstruct backoff: nil,
            chan: nil,
            chan_ref: nil,
            conn: nil,
            index: %{},
            opts: []

  @typedoc "A list of headers"
  @type headers() :: list()

  @typedoc "The payload of a request or a response."
  @type payload() :: String.t()

  @typedoc "A handler for managing requests."
  @type message_handler() ::
          (pid(), payload(), headers() -> :ok | :error)
          | {:module, :atom}
          | {:module, :atom, any()}

  @typedoc """
  A set of options available to configure the *AMQP Consumer*.

  **Note** that `queue` option is required.

  * `backoff`: how the backoff delay should behave. More info at
  `t:Backoff.options/0`.

  * `connection_name`: the name of the connections created by the *AMQP
  Consumer*.

  * `connection_options`: configures the broker connection. All available
  options are available at `AMQP.Connection.open/2`.

  * `qos_options`: configures the consumer QoS options. More info
  at `AMQP.Basic.qos/2`. `global` option does not apply in this context.

  * `queue`: the name of the queue for consuming requests.  **Required**.

  * `message_handler`: the function of the type `t:message_handler/0` that will
  handle request received by the *Consumer*.

  * `request_handler_supervisor`: an instance of a `Task.Supervisor` to
  supervise handlers. The *Consumer* will use this supervisor to run request
  handlers, managing any kind of error, raise or exit produced by the handler.
  """
  @type option() ::
          {:backoff, Backoff.options()}
          | {:connection_name, String.t()}
          | {:connection_options, keyword() | String.t()}
          | {:message_handler, message_handler()}
          | {:message_handler_supervisor, Supervisor.supervisor()}
          | {:qos_options, keyword()}
          | {:queue, String.t()}

  @typedoc "A list of `t:option/0`s."
  @type options() :: [option() | {atom(), any()}]

  @required_options [:queue]

  #
  # Client
  #

  @doc "Starts a new *AMQP* consumer-producer."
  @spec start_link(options()) :: GenServer.on_start()
  def start_link(opts) do
    check_required_opts!(opts)
    GenServer.start_link(__MODULE__, opts, [])
  end

  #
  # Server
  #

  @impl true
  def init(opts), do: {:ok, new_state(opts), {:continue, :try_connect}}

  @impl true
  def terminate(reason, state = %State{opts: opts}) do
    Logger.error("Terminating: #{inspect(reason)}")

    close_channel(opts, state.chan)
    close_connection(opts, state.conn)
  end

  #
  # Server - Handle Info - AMQP Setup
  #

  @impl true
  def handle_info(:try_connect, state = %State{opts: opts}) do
    conn_opts = Keyword.get(opts, :connection_options, [])

    case amqp(opts).open_connection(conn_opts, connection_name(opts)) do
      {:ok, conn} ->
        Logger.info("AMQP connection established.")
        {:noreply, state, {:continue, {:setup_connection, conn}}}

      {:error, reason} ->
        {:noreply, state, {:continue, {:retry_connect, reason}}}

      # NOTE: Undocumented AMQP lib error.
      _error ->
        {:noreply, state, {:continue, {:retry_connect, "unknown error"}}}
    end
  end

  def handle_info(:try_open_channel, state = %State{opts: opts}) do
    qos_options = Keyword.get(opts, :qos_options, [])

    case amqp(opts).open_channel(state.conn, qos_options) do
      {:ok, chan} ->
        Logger.info("AMQP channels open.")
        {:noreply, state, {:continue, {:setup_channel, chan}}}

      {:error, reason} ->
        {:noreply, state, {:continue, {:retry_open_channel, reason}}}
    end
  end

  def handle_info(:try_consume, state = %State{chan: chan, opts: opts}) do
    queue = Keyword.fetch!(opts, :queue)

    case amqp(opts).consume(chan, queue) do
      {:ok, _ctag} ->
        # NOTE: Wait for :basic_consume_ok to acknowledge the subscription
        {:noreply, state}

      {:error, reason} ->
        {:noreply, state, {:continue, {:retry_consume, reason}}}
    end
  end

  #
  # Server - Handle Info - AMQP Monitoring
  #

  # Connection closed
  def handle_info({:DOWN, _, :process, pid, reason}, state = %State{conn: %{pid: pid}}) do
    {:stop, {:connection_lost, :consumer, reason}, %{state | conn: nil}}
  end

  # Channel closed
  def handle_info({:DOWN, _, :process, pid, reason}, state = %State{chan: %{pid: pid}}) do
    {:noreply, reset_channel(state), {:continue, {:retry_open_channel, reason}}}
  end

  #
  # Server - Handle Info - AMQP Events
  #

  # Consuming canceled remotely
  def handle_info({:basic_cancel, _info}, state) do
    {:noreply, state, {:continue, {:retry_consume, "canceled remotely"}}}
  end

  # Consuming starts
  def handle_info({:basic_consume_ok, _info}, state = %State{opts: opts}) do
    queue = Keyword.fetch!(opts, :queue)
    Logger.info("Consuming requests from #{queue} queue.")
    {:noreply, reset_backoff(state)}
  end

  # A new message arrives
  def handle_info({:basic_deliver, payload, meta}, state = %State{opts: opts}) do
    Logger.debug("New request: #{inspect(payload)}, #{inspect(meta)}.")

    case Keyword.fetch(opts, :message_handler) do
      {:ok, message_handler} ->
        headers = to_request_headers(meta)
        {module, fun, args} = to_mfa(message_handler, [self(), payload, headers])

        :telemetry.execute([:plug_amqp, :consumer, :incoming_message], %{
          size: byte_size(payload)
        })

        task =
          case Keyword.get(opts, :request_handler_supervisor) do
            nil -> Task.async(module, fun, args)
            supervisor -> Task.Supervisor.async_nolink(supervisor, module, fun, args)
          end

        {:noreply, put_index_entry(state, task, meta)}

      :error ->
        Logger.warn("No request handler configured")
        {:noreply, state}
    end
  end

  #
  # Server - Handle Info - Task Handlers
  #

  # A message handler finish successfully
  def handle_info({ref, :ok}, state = %State{index: index, opts: opts})
      when is_reference(ref) do
    {_task, meta} = Map.fetch!(index, ref)

    demonitor(ref)

    ack(opts, state.chan, meta)

    {:noreply, delete_index_entry(state, ref)}
  end

  # A message handler finish successfully
  def handle_info({ref, :ok}, state = %State{index: index, opts: opts})
      when is_reference(ref) do
    {_task, meta} = Map.fetch!(index, ref)

    demonitor(ref)

    ack(opts, state.chan, meta)

    {:noreply, delete_index_entry(state, ref)}
  end

  # A message handler returns an non expected response
  def handle_info({ref, reason}, state) when is_reference(ref) do
    demonitor(ref)

    {:noreply, state, {:continue, {:handle_task_error, ref, reason}}}
  end

  # A message handler fails
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {:noreply, state, {:continue, {:handle_task_error, ref, reason}}}
  end

  #
  # Server - Handle Continue - AMQP Setup
  #

  @impl true
  def handle_continue({:setup_connection, conn}, state) do
    Process.monitor(conn.pid)

    send(self(), :try_open_channel)

    {:noreply,
     state
     |> Map.put(:conn, conn)
     |> reset_backoff()}
  end

  def handle_continue(:try_connect, state), do: handle_info(:try_connect, state)

  def handle_continue({:retry_connect, reason}, state) do
    {delay, next_backoff} = Backoff.step(state.backoff)
    Logger.error("Cannot open an AMQP connection: #{inspect(reason)}. Retrying in #{delay}ms.")
    Process.send_after(self(), :try_connect, delay)
    :telemetry.execute([:plug_amqp, :consumer, :retry], %{reason: :connection})

    {:noreply, %{state | backoff: next_backoff}}
  end

  def handle_continue({:setup_channel, chan}, state) do
    chan_ref = Process.monitor(chan.pid)

    send(self(), :try_consume)

    # NOTE: Avoid resetting backoff here, reset when subscription succeeds
    {:noreply, put_channel(state, chan, chan_ref)}
  end

  def handle_continue({:retry_consume, reason}, state = %State{opts: opts}) do
    queue = Keyword.fetch!(opts, :queue)
    {delay, next_backoff} = Backoff.step(state.backoff)
    Logger.error("Cannot consume from #{queue}: #{inspect(reason)}. Retrying in #{delay}ms.")
    Process.send_after(self(), :try_consume, delay)
    :telemetry.execute([:plug_amqp, :consumer, :retry], %{reason: :consume})

    {:noreply, %{state | backoff: next_backoff}}
  end

  def handle_continue({:retry_open_channel, reason}, state = %State{}) do
    Enum.each(state.index, fn {_ref, {task, _meta}} -> Task.shutdown(task, :brutal_kill) end)
    {delay, next_backoff} = Backoff.step(state.backoff)
    Logger.error("Cannot open an AMQP channel: #{inspect(reason)}. Retrying in #{delay}ms.")
    Process.send_after(self(), :try_open_channel, delay)
    :telemetry.execute([:plug_amqp, :consumer, :retry], %{reason: :channel})
    {:noreply, %{state | backoff: next_backoff, index: %{}}}
  end

  #
  # Server - Handle Continue - Message Handlers
  #

  def handle_continue({:handle_task_error, ref, reason}, state = %State{index: index, opts: opts}) do
    {_task, meta} = Map.fetch!(index, ref)
    id = get_request_id(meta)
    Logger.error("Request handler for request #{id} failed: #{inspect(reason)}.")

    nack_or_reject(opts, state.chan, meta)

    {:noreply, delete_index_entry(state, ref)}
  end

  #
  # Headers Helpers
  #

  @spec to_request_headers(map()) :: list()
  defp to_request_headers(%{headers: meta_headers}), do: meta_headers

  #
  # State Helpers
  #

  @spec delete_index_entry(t(), reference()) :: t()
  defp delete_index_entry(state = %State{index: index}, ref) do
    %{state | index: Map.delete(index, ref)}
  end

  @spec new_state(options()) :: State.t()
  defp new_state(opts) do
    backoff =
      opts
      |> Keyword.get(:backoff, [])
      |> Backoff.new()

    %State{backoff: backoff, opts: opts}
  end

  @spec put_channel(t(), Channel.t(), reference()) :: t()
  defp put_channel(state, chan, chan_ref) do
    %{
      state
      | chan: chan,
        chan_ref: chan_ref
    }
  end

  @spec put_index_entry(t(), Task.t(), map()) :: t()
  defp put_index_entry(state = %State{index: index}, task = %Task{ref: ref}, meta) do
    %{state | index: Map.put(index, ref, {task, meta})}
  end

  @spec reset_channel(t()) :: t()
  defp reset_channel(state) do
    %{state | chan: nil, chan_ref: nil}
  end

  @spec reset_backoff(t()) :: t()
  defp reset_backoff(state = %State{backoff: backoff}) do
    %{state | backoff: Backoff.reset(backoff)}
  end

  #
  # AMQP Helpers
  #

  @spec amqp(keyword()) :: module()
  defp amqp(opts), do: Keyword.get(opts, :amqp_backend, Plug.AMQP.Backends.AMQP)

  @spec ack(keyword(), Channel.t(), map()) :: :ok
  defp ack(opts, chan, %{delivery_tag: tag}) do
    with {:error, reason} <- amqp(opts).ack(chan, tag) do
      Logger.error("Error acknowledging message #{tag}: #{inspect(reason)}")
    end
  end

  @spec connection_name(keyword()) :: String.t()
  defp connection_name(opts) do
    opts
    |> Keyword.get_lazy(:connection_name, &connection_name/0)
    |> to_string()
  end

  @spec connection_name() :: String.t()
  defp connection_name(), do: "#{__MODULE__}.#{:erlang.pid_to_list(self())}"

  @spec close_connection(keyword(), Connection.t()) :: :ok | {:error, any()}
  defp close_connection(_opts, nil), do: :ok
  defp close_connection(opts, conn = %Connection{}), do: amqp(opts).close_connection(conn)

  @spec close_channel(keyword(), Channel.t()) :: :ok | {:error, any()}
  defp close_channel(_opts, nil), do: :ok
  defp close_channel(opts, chan = %Channel{}), do: amqp(opts).close_channel(chan)

  @spec nack_or_reject(keyword(), Channel.t(), map()) :: :ok
  defp nack_or_reject(opts, chan, meta = %{delivery_tag: tag, redelivered: redelivered}) do
    if redelivered do
      id = get_request_id(meta)
      Logger.warn("Message #{id} was processed at least two times. Discarding.")
      do_nack(opts, chan, tag, requeue: false)
    else
      do_nack(opts, chan, tag)
    end
  end

  @spec do_nack(keyword(), Channel.t(), Basic.delivery_tag(), keyword()) :: :ok
  defp do_nack(opts, chan, tag, nack_opts \\ []) do
    with {:error, reason} <- amqp(opts).nack(chan, tag, nack_opts) do
      Logger.error("Error non acknowledging request #{tag}: #{inspect(reason)}")
    end
  end

  #
  # Miscellaneous
  #

  @spec check_required_opts!(keyword()) :: :ok | no_return()
  defp check_required_opts!(opts) do
    Enum.each(@required_options, fn key ->
      if !Keyword.has_key?(opts, key) do
        raise ArgumentError, message: "missing required #{key} option"
      end
    end)
  end

  @spec demonitor(reference() | nil) :: boolean()
  defp demonitor(ref) when is_reference(ref), do: Process.demonitor(ref, [:flush])

  @spec get_request_id(map()) :: String.t()
  defp get_request_id(%{message_id: id}) when id != :undefined, do: id
  defp get_request_id(%{delivery_tag: tag}), do: tag
  defp get_request_id(_envelope), do: "unknown"

  @spec to_mfa({module(), atom()} | fun(), [any()]) :: {module(), atom(), [any()]}
  defp to_mfa({module, fun, arg}, args), do: {module, fun, args ++ [arg]}
  defp to_mfa({module, fun}, args), do: {module, fun, args}
  defp to_mfa(fun, args), do: {:erlang, :apply, [fun, args]}
end

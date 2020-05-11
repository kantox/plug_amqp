defmodule Plug.AMQP.ConsumerProducer do
  @moduledoc ~s"""
  A consumer-producer for RPC over *AMQP* protocol.

  This module provides a consumer-producer for managing remote procedure calls
  over *AMQP*. A consumer-producer creates at least one connection to an *AMQP*
  broker. Request are received by consuming from the given queue. Responses are
  routed based on the `reply_to` message header.

  The response properties mirrors the properties of the request. For example,
  if a request is sent using persistence, the response will be sent using
  persistence too.

  ## Usage

  To use this consumer-producer we must provide a `t:request_handler/0`
  function. The consumer-producer will call this function to manage requests.
  Handlers can use `send_resp/3` to return a response of the request producer
  (client). Check `t:options/0` to get more info about all the options
  available.

  ## Example

  **Note** that we avoid creating a response queue here using the
  [RabbitMQ Direct Reply-To](https://www.rabbitmq.com/direct-reply-to.html)
  extension.

  ```elixir
  #{File.read!("examples/consumer_producer.exs")}
  ```
  """
  use GenServer

  alias AMQP.{Basic, Channel, Connection}
  alias __MODULE__, as: State

  require Logger

  @typep t() :: %State{
           backoff: Backoff.t(),
           consumer_chan: Channel.t() | nil,
           consumer_chan_ref: reference() | nil,
           consumer_conn: Connection.t() | nil,
           index: %{reference() => {Task.t(), keyword()}},
           opts: options(),
           producer_chan: Channel.t() | nil,
           producer_chan_ref: reference() | nil,
           producer_conn: Connection.t() | nil
         }
  defstruct backoff: nil,
            consumer_chan: nil,
            consumer_chan_ref: nil,
            consumer_conn: nil,
            index: %{},
            opts: [],
            producer_chan: nil,
            producer_chan_ref: nil,
            producer_conn: nil

  @typedoc "A header of a request or a response"
  @type header() :: {String.t(), binary() | number()}

  @typedoc "A list of headers"
  @type headers() :: [header()]

  @typedoc "The payload of a request or a response."
  @type payload() :: String.t()

  @typedoc "A handler for managing requests."
  @type request_handler() ::
          (pid(), payload(), headers() -> :ok | any())
          | {:module, :atom}
          | {:module, :atom, any()}

  @typedoc "A response of a request."
  @type response() :: {:ok, payload(), headers()} | {:error, any()}

  @typedoc """
  A set of options available to configure the *AMQP Consumer-Producer*.

  **Note** that `consumer_queue` option is required.

  * `backoff`: how the backoff delay should behave. More info at
    `t:Backoff.options/0`.

  * `connection_name`: the name of the connections created by the *AMQP
     Consumer-Producer*. Note that the *Consumer-Producer* can create several
     connections. In that case, the provided connection name will be used as a
     prefix.

  * `connection_options`: configures the broker connection. All available
     options are available at `AMQP.Connection.open/2`.

  * `consumer_qos_options`: configures the consumer QoS options. More info
     at `AMQP.Basic.qos/2`. `global` option does not apply in this context.

  * `consumer_queue`: the name of the queue for consuming requests.
     **Required**.

  * `request_handler`: the function of the type `t:request_handler/0` that will
     handle request received by the *Consumer-Producer*.

  * `request_handler_supervisor`: an instance of a `Task.Supervisor` to
     supervise handlers. The *Consumer-Producer* will use this supervisor to
     run request handlers, managing any kind of error, raise or exit produced
     by the handler.
  """
  @type option() ::
          {:backoff, Backoff.options()}
          | {:connection_name, String.t()}
          | {:connection_options, keyword() | String.t()}
          | {:consumer_qos_options, keyword()}
          | {:consumer_queue, String.t()}
          | {:request_handler, request_handler()}
          | {:request_handler_supervisor, Supervisor.supervisor()}

  @typedoc "A list of `t:option/0`s."
  @type options() :: [option() | {atom(), any()}]

  @required_options [:consumer_queue]

  #
  # Client
  #

  @doc "Starts a new *AMQP* consumer-producer."
  @spec start_link(options()) :: GenServer.on_start()
  def start_link(opts) do
    check_required_opts!(opts)
    GenServer.start_link(__MODULE__, opts, [])
  end

  @doc "Sends a response from a request handler"
  @spec send_resp(GenServer.server(), payload(), headers()) :: :ok
  def send_resp(server, payload, headers \\ []) do
    send(server, {:send_resp, self(), payload, headers})
  end

  #
  # Server
  #

  @impl true
  def init(opts), do: {:ok, new_state(opts), {:continue, :try_connect}}

  @impl true
  def terminate(reason, state = %State{opts: opts}) do
    Logger.error("Terminating: #{inspect(reason)}")

    close_channel(opts, state.consumer_chan)
    close_channel(opts, state.producer_chan)
    close_connection(opts, state.consumer_conn)
    close_connection(opts, state.producer_conn)
  end

  #
  # Server - Handle Info - AMQP Setup
  #

  @impl true
  def handle_info(:try_connect, state = %State{opts: opts}) do
    conn_opts = Keyword.get(opts, :connection_options, [])

    consumer_conn_res = amqp(opts).open_connection(conn_opts, connection_name(:consumer, opts))
    producer_conn_res = amqp(opts).open_connection(conn_opts, connection_name(:producer, opts))

    case {consumer_conn_res, producer_conn_res} do
      {{:ok, consumer_conn}, {:ok, producer_conn}} ->
        Logger.info("AMQP connection established.")
        {:noreply, state, {:continue, {:setup_connections, consumer_conn, producer_conn}}}

      {{:error, reason}, _producer_conn_res} ->
        {:noreply, state, {:continue, {:retry_connect, reason}}}

      {_consumer_conn_res, {:error, reason}} ->
        {:noreply, state, {:continue, {:retry_connect, reason}}}

      # NOTE: Undocumented AMQP lib error.
      _error ->
        {:noreply, state, {:continue, {:retry_connect, "unknown error"}}}
    end
  end

  def handle_info(:try_open_channels, state = %State{opts: opts}) do
    consumer_qos_options = Keyword.get(opts, :consumer_qos_options, [])

    consumer_chan_res = amqp(opts).open_channel(state.consumer_conn, consumer_qos_options)
    producer_chan_res = amqp(opts).open_channel(state.producer_conn, [])

    case {consumer_chan_res, producer_chan_res} do
      {{:ok, consumer_chan}, {:ok, producer_chan}} ->
        Logger.info("AMQP channels open.")
        {:noreply, state, {:continue, {:setup_channels, consumer_chan, producer_chan}}}

      {{:error, reason}, _producer_chan_res} ->
        {:noreply, state, {:continue, {:retry_open_channels, reason}}}

      {_consumer_chan_res, {:error, reason}} ->
        {:noreply, state, {:continue, {:retry_open_channels, reason}}}
    end
  end

  def handle_info(:try_consume, state = %State{consumer_chan: consumer_chan, opts: opts}) do
    queue = Keyword.fetch!(opts, :consumer_queue)

    case amqp(opts).consume(consumer_chan, queue) do
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

  # Consumer connection closed
  def handle_info({:DOWN, _, :process, pid, reason}, state = %State{consumer_conn: %{pid: pid}}) do
    {:stop, {:connection_lost, :consumer, reason}, %{state | consumer_conn: nil}}
  end

  # Producer connection closed
  def handle_info({:DOWN, _, :process, pid, reason}, state = %State{producer_conn: %{pid: pid}}) do
    {:stop, {:connection_lost, :producer, reason}, %{state | producer_conn: nil}}
  end

  # Consumer channel closed
  def handle_info({:DOWN, _, :process, pid, reason}, state = %State{consumer_chan: %{pid: pid}}) do
    demonitor(state.producer_chan_ref)
    close_channel(state.opts, state.producer_chan)

    {:noreply, reset_channels(state), {:continue, {:retry_open_channels, reason}}}
  end

  # Producer channel closed
  def handle_info({:DOWN, _, :process, pid, reason}, state = %State{producer_chan: %{pid: pid}}) do
    demonitor(state.consumer_chan_ref)
    close_channel(state.opts, state.consumer_chan)

    {:noreply, reset_channels(state), {:continue, {:retry_open_channels, reason}}}
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
    queue = Keyword.fetch!(opts, :consumer_queue)
    Logger.info("Consuming requests from #{queue} queue.")
    {:noreply, reset_backoff(state)}
  end

  # A new request arrives
  def handle_info({:basic_deliver, payload, meta}, state = %State{opts: opts}) do
    Logger.debug("New request: #{inspect(payload)}, #{inspect(meta)}.")

    case Keyword.fetch(opts, :request_handler) do
      {:ok, request_handler} ->
        headers = to_request_headers(meta)
        {module, fun, args} = to_mfa(request_handler, [self(), payload, headers])

        :telemetry.execute([:plug_amqp, :consumer_producer, :incoming_message], %{
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
  # Server - Handle Info - Request Handlers
  #

  # A request handler finish successfully
  def handle_info({ref, :ok}, state = %State{index: index, opts: opts})
      when is_reference(ref) do
    {_task, meta} = Map.fetch!(index, ref)

    demonitor(ref)

    ack(opts, state.consumer_chan, meta)

    {:noreply, delete_index_entry(state, ref)}
  end

  # A request handler returns an non expected response
  def handle_info({ref, reason}, state) when is_reference(ref) do
    demonitor(ref)

    {:noreply, state, {:continue, {:handle_task_error, ref, reason}}}
  end

  # A request handler fails
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    {:noreply, state, {:continue, {:handle_task_error, ref, reason}}}
  end

  #
  # Server - Handle Continue - AMQP Setup
  #

  @impl true
  def handle_continue({:setup_connections, consumer_conn, producer_conn}, state) do
    Process.monitor(consumer_conn.pid)
    Process.monitor(producer_conn.pid)

    send(self(), :try_open_channels)

    {:noreply,
     state
     |> put_connections(consumer_conn, producer_conn)
     |> reset_backoff()}
  end

  def handle_continue(:try_connect, state), do: handle_info(:try_connect, state)

  def handle_continue({:retry_connect, reason}, state) do
    {delay, next_backoff} = Backoff.step(state.backoff)
    Logger.error("Cannot open an AMQP connection: #{inspect(reason)}. Retrying in #{delay}ms.")
    Process.send_after(self(), :try_connect, delay)
    :telemetry.execute([:plug_amqp, :consumer_producer, :retry], %{reason: :connection})

    {:noreply, %{state | backoff: next_backoff}}
  end

  def handle_continue({:setup_channels, consumer_chan, producer_chan}, state) do
    consumer_ref = Process.monitor(consumer_chan.pid)
    producer_ref = Process.monitor(producer_chan.pid)

    send(self(), :try_consume)

    # NOTE: Avoid resetting backoff here, reset when subscription succeeds
    {:noreply, put_channels(state, consumer_chan, consumer_ref, producer_chan, producer_ref)}
  end

  def handle_continue({:retry_consume, reason}, state = %State{opts: opts}) do
    queue = Keyword.fetch!(opts, :consumer_queue)
    {delay, next_backoff} = Backoff.step(state.backoff)
    Logger.error("Cannot consume from #{queue}: #{inspect(reason)}. Retrying in #{delay}ms.")
    Process.send_after(self(), :try_consume, delay)
    :telemetry.execute([:plug_amqp, :consumer_producer, :retry], %{reason: :consume})

    {:noreply, %{state | backoff: next_backoff}}
  end

  def handle_continue({:retry_open_channels, reason}, state = %State{index: index}) do
    Enum.each(index, fn {_ref, {task, _meta}} -> Task.shutdown(task, :brutal_kill) end)
    {delay, next_backoff} = Backoff.step(state.backoff)
    Logger.error("Cannot open an AMQP channel: #{inspect(reason)}. Retrying in #{delay}ms.")
    Process.send_after(self(), :try_open_channels, delay)
    :telemetry.execute([:plug_amqp, :consumer_producer, :retry], %{reason: :channel})

    {:noreply, %{state | backoff: next_backoff, index: %{}}}
  end

  #
  # Server - Handle Continue - Requests Handlers
  #

  def handle_continue({:handle_task_error, ref, reason}, state = %State{index: index, opts: opts}) do
    {_task, meta} = Map.fetch!(index, ref)
    id = get_request_id(meta)
    Logger.error("Request handler for request #{id} failed: #{inspect(reason)}.")

    nack_or_reject(opts, state.consumer_chan, meta)

    {:noreply, delete_index_entry(state, ref)}
  end

  #
  # Server - Handle Cast
  #

  @impl true
  def handle_info(
        {:send_resp, caller, payload, headers},
        state = %State{index: index, opts: opts}
      ) do
    case Enum.find(index, fn {_k, v} -> match?({%Task{pid: ^caller}, _meta}, v) end) do
      nil ->
        Logger.warn("Response sent from an unknown task")

      {_ref, {_task, req_meta = %{reply_to: routing_key}}} ->
        resp_meta = [
          correlation_id: req_meta.correlation_id || req_meta.message_id,
          headers: to_arguments(headers),
          persistent: req_meta.persistent,
          timestamp: DateTime.utc_now() |> DateTime.to_unix()
        ]

        case amqp(opts).publish(state.producer_chan, "", routing_key, payload, resp_meta) do
          :ok ->
            :telemetry.execute([:plug_amqp, :consumer_producer, :outcoming_message], %{
              size: byte_size(payload)
            })

          {:error, reason} ->
            id = get_request_id(req_meta)
            Logger.error("Error sending response of message #{id}: #{inspect(reason)}.")
            nack_or_reject(opts, state.consumer_chan, req_meta)
        end
    end

    {:noreply, state}
  end

  #
  # Headers Helpers
  #

  @bubbling_meta_headers ~w|app_id content_encoding content_type message_id routing_key type|a

  @spec to_request_headers(map()) :: headers()
  defp to_request_headers(request_meta = %{headers: meta_headers}) do
    request_headers = from_arguments(meta_headers)

    bubbling_headers =
      @bubbling_meta_headers
      |> Stream.map(&{amqp_meta_key_to_header_key(&1), Map.get(request_meta, &1)})
      |> Stream.reject(&(elem(&1, 1) == :undefined))
      |> Enum.to_list()

    # NOTE: Order matters. Clients can use Enum.into(headers, %{}) to ignore
    # duplicates.
    request_headers ++ bubbling_headers
  end

  @spec amqp_meta_key_to_header_key(atom()) :: String.t()
  defp amqp_meta_key_to_header_key(meta_key) do
    suffix =
      meta_key
      |> Atom.to_string()
      |> String.replace("_", "-")

    "amqp-#{suffix}"
  end

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

  @spec put_channels(t(), Channel.t(), reference(), Channel.t(), reference()) :: t()
  defp put_channels(state, consumer_chan, consumer_ref, producer_chan, producer_ref) do
    %{
      state
      | consumer_chan: consumer_chan,
        consumer_chan_ref: consumer_ref,
        producer_chan: producer_chan,
        producer_chan_ref: producer_ref
    }
  end

  @spec put_connections(t(), Connection.t(), Connection.t()) :: t()
  defp put_connections(state, consumer_conn, producer_conn) do
    %{state | consumer_conn: consumer_conn, producer_conn: producer_conn}
  end

  @spec put_index_entry(t(), Task.t(), map()) :: t()
  defp put_index_entry(state = %State{index: index}, task = %Task{ref: ref}, meta) do
    %{state | index: Map.put(index, ref, {task, meta})}
  end

  @spec reset_channels(t()) :: t()
  defp reset_channels(state) do
    %{
      state
      | consumer_chan: nil,
        consumer_chan_ref: nil,
        producer_chan: nil,
        producer_chan_ref: nil
    }
  end

  @spec reset_backoff(t()) :: t()
  defp reset_backoff(state = %State{backoff: backoff}) do
    %{state | backoff: Backoff.reset(backoff)}
  end

  #
  # AMQP Arguments Helpers
  #

  # TODO: Add support for remaining types.
  # FIXME: Boolean types are not working.
  @spec from_argument({String.t(), AMQP.argument_type(), term()}) :: header()
  defp from_argument({name, :longstr, value}), do: {name, value}
  defp from_argument({name, :signedint, value}), do: {name, value}
  defp from_argument({name, :double, value}), do: {name, value}
  defp from_argument({name, :float, value}), do: {name, value}
  defp from_argument({name, :long, value}), do: {name, value}
  defp from_argument({name, :short, value}), do: {name, value}
  defp from_argument({name, _kind, value}), do: {name, to_string(value)}

  @spec from_arguments(AMQP.arguments() | :undefined) :: headers()
  defp from_arguments(:undefined), do: []
  defp from_arguments(arguments), do: Enum.map(arguments, &from_argument/1)

  @spec to_argument(header()) :: {String.t(), AMQP.argument_type(), term()}
  defp to_argument({name, value}) when is_atom(value), do: {name, :longstr, value}
  defp to_argument({name, value}) when is_binary(value), do: {name, :longstr, value}
  defp to_argument({name, value}) when is_integer(value), do: {name, :signedint, value}
  defp to_argument({name, value}) when is_float(value), do: {name, :double, value}
  defp to_argument({name, value}), do: {name, :longstr, to_string(value)}

  @spec to_arguments(headers()) :: AMQP.arguments()
  defp to_arguments(headers), do: Enum.map(headers, &to_argument/1)

  #
  # AMQP Helpers
  #

  @spec amqp(keyword()) :: module()
  defp amqp(opts), do: Keyword.get(opts, :amqp_backend, Plug.AMQP.Backends.AMQP)

  @spec ack(keyword(), Channel.t(), map()) :: :ok
  defp ack(opts, chan, %{delivery_tag: tag}) do
    with {:error, reason} <- amqp(opts).ack(chan, tag) do
      Logger.error("Error acknowledging request #{tag}: #{inspect(reason)}")
    end
  end

  @spec connection_name(atom(), keyword()) :: String.t()
  defp connection_name(:consumer, opts), do: "#{connection_name(opts)}.Consumer"
  defp connection_name(:producer, opts), do: "#{connection_name(opts)}.Producer"

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
      Logger.warn("Request #{id} was processed at least two times. Discarding.")
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
  defp demonitor(nil), do: false
  defp demonitor(ref) when is_reference(ref), do: Process.demonitor(ref, [:flush])

  defp get_request_id(%{message_id: id}) when id != :undefined, do: id
  defp get_request_id(%{delivery_tag: tag}), do: tag

  @spec to_mfa({module(), atom()} | fun(), [any()]) :: {module(), atom(), [any()]}
  defp to_mfa({module, fun, arg}, args), do: {module, fun, args ++ [arg]}
  defp to_mfa({module, fun}, args), do: {module, fun, args}
  defp to_mfa(fun, args), do: {:erlang, :apply, [fun, args]}
end

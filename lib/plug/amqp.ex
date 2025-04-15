defmodule Plug.Amqp do
  @moduledoc """
  TODO
  """

  use Supervisor

  require Logger

  alias AMQPHelpers.Reliability.{Consumer, Producer}
  alias Plug.AMQP.Conn

  @supervisor_options ~w(strategy max_restarts max_seconds name)a

  # TODO: Add support to url option, like "amqp://my_broker:5672".

  @typedoc "TODO"
  @type option ::
          Supervisor.option()
          | Supervisor.init_option()
          | {:consumer_options, Consumer.options()}
          | {:plug, module() | {module() | keyword()}}
          | {:producer_options, Consumer.options()}

  @typedoc "TODO"
  @type options :: [option()]

  @spec start_link(options()) :: Supervisor.on_start()
  def start_link(options \\ []) do
    {supervisor_opts, plug_amqp_opts} = Keyword.split(options, @supervisor_options)

    Supervisor.start_link(__MODULE__, plug_amqp_opts, supervisor_opts)
  end

  @impl true
  def init(opts) do
    supervisor = self()

    consumer_opts =
      opts
      |> Keyword.fetch!(:consumer_options)
      |> Keyword.put(:message_handler, &handle_message(supervisor, &1, &2, opts))
      |> Keyword.put(:requeue, get_in(opts, [:consumer_options, :requeue]))
      |> Keyword.put(:task_supervisor, {:via, __MODULE__, {supervisor, Task.Supervisor}})

    producer_opts = Keyword.fetch!(opts, :producer_options)

    children = [
      {Consumer, consumer_opts},
      {Producer, producer_opts},
      Task.Supervisor
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  #
  # Name Registry Implementation
  #

  @doc false
  def register_name(_name, _pid), do: :yes

  @doc false
  def unregister_name(name), do: name

  @doc false
  def whereis_name({supervisor, module}), do: whereis_name(supervisor, module)

  @doc false
  def whereis_name(supervisor, module) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.find(&match?({^module, _pid, _kind, _modules}, &1))
    |> elem(1)
  end

  #
  # Message Handling Implementation
  #

  # TODO: Add more options to customize the nack of the messages.

  @spec handle_message(pid(), binary(), map(), options()) :: :ok | {:error, term()}
  defp handle_message(supervisor, payload, meta, opts) do
    producer = whereis_name(supervisor, Producer)

    {plug, plug_opts} = fetch_plug!(opts)
    conn = Conn.conn(producer, payload, meta)

    try do
      conn
      |> plug.call(plug_opts)
      |> send_resp()
    catch
      _kind, reason ->
        Logger.error("Cannot handle message from Plug.AMQP",
          reason: reason,
          stacktace: __STACKTRACE__
        )

        {:error, reason}
    else
      _conn ->
        :ok
    end
  end

  #
  # Misc
  #

  @spec fetch_plug!(options()) :: {module(), keyword()} | no_return()
  defp fetch_plug!(opts) do
    case Keyword.fetch!(opts, :plug) do
      {module, opts} -> {module, opts}
      module -> {module, []}
    end
  end

  defp send_resp(conn = %Plug.Conn{state: :set}), do: Plug.Conn.send_resp(conn)
  defp send_resp(conn = %Plug.Conn{}), do: conn
end

defmodule Plug.AMQP do
  @moduledoc """
  Adapter interface to the [AMQP RPC pattern](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html).

  `Plug.AMQP` provides a [AMQP](https://www.amqp.org) interface to `Plug`.
  When using `Plug.AMQP` you can write servers that answer requests sent through
  an *AMQP* broker, like [RabbitMQ](https://www.rabbitmq.com). The request
  response pattern is explained in detail [here](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html).

  ## Usage

  To use `Plug.AMQP`, add it to your supervision tree by (assuming that your
  Plug module is named `MyPlug`) :

      children = [
        {Plug.AMQP, connection_options: "amqp://my-rabbit:5672", plug: MyPlug}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  Check `t:option/0` and `t:Plug.AMQP.ConsumerProducer.option/0` for more
  options.

  ## Examples

  The following example is taken from the
  [RabbitMQ RPC Tutorial](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html)
  but using `Plug.AMQP`.

  ```elixir
  #{File.read!("examples/fibonacci.exs")}
  ```

  """
  use Supervisor

  alias Plug.AMQP.{Conn, ConsumerProducer}

  @typedoc """
  A `Plug.AMQP` configuration option.

  `Plug.AMQP` supports any of `t:Plug.AMQP.ConsumerProducer.option/0`. Also, the `plug`
  option must be used to set the main plug of a server.
  """
  @type option() ::
          {:plug, module() | {module() | keyword()}}
          | ConsumerProducer.option()

  @typedoc "A list of `t:option/0`s."
  @type options() :: [option() | {atom(), any()}]

  @doc false
  @spec start_link(keyword) :: Supervisor.on_start()
  def start_link(opts) do
    with {:ok, supervisor} <- Supervisor.start_link(__MODULE__, opts, []),
         :ok <- start_children(supervisor, opts) do
      {:ok, supervisor}
    end
  end

  @impl true
  def init(_opts) do
    Supervisor.init([], strategy: :one_for_one)
  end

  @doc false
  @spec handle(
          GenServer.server(),
          ConsumerProducer.payload(),
          ConsumerProducer.headers(),
          options()
        ) :: :ok
  def handle(endpoint, payload, headers, opts) do
    {plug, plug_opts} = fetch_plug!(opts)
    conn = Conn.conn(endpoint, payload, headers)

    plug.call(conn, plug_opts)

    :ok
  end

  @spec start_children(Supervisor.supervisor(), keyword()) :: :ok | {:error, any()}
  defp start_children(supervisor, opts) do
    with {:ok, task_supervisor} <- Supervisor.start_child(supervisor, Task.Supervisor),
         opts <-
           opts
           |> Keyword.put_new(:request_handler_supervisor, task_supervisor)
           |> Keyword.put(:request_handler, {__MODULE__, :handle, opts}),
         {:ok, _endpoint} <- Supervisor.start_child(supervisor, {ConsumerProducer, opts}) do
      :ok
    end
  end

  @spec fetch_plug!(options()) :: {module(), keyword()} | no_return()
  defp fetch_plug!(opts) do
    case Keyword.fetch!(opts, :plug) do
      {module, opts} -> {module, opts}
      module -> {module, []}
    end
  end
end

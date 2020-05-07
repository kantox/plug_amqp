defmodule Plug.AMQP.Backends.AMQP do
  @moduledoc false

  @behaviour Plug.AMQP.Backend

  @impl true
  defdelegate ack(chan, delivery_tag), to: AMQP.Basic

  @impl true
  defdelegate close_channel(channel), to: AMQP.Channel, as: :close

  @impl true
  defdelegate close_connection(connection), to: AMQP.Connection, as: :close

  # NOTE: `AMQP.Basic.consume` does not work correctly (or at least, in a
  # deterministic fashion) when the given queue does not allow consuming
  # (for example, exclusive queues) or the queue is not created, or a policy
  # avoids consuming there. If you are lucky, you will get an error when calling
  # the function, if not, the channel will crash after receiving some messages
  # or silently ignores the error (so no subscription and no error). The
  # `no_wait` adds even more indeterminism to equation, disabling
  # `basic_consume_ok` message.
  #
  # We implement a workaround here. First, we create a new channel, and then
  # we try to get the status of the queue. The procedure is monitored by another
  # process. Check out `try_get_status/2` for more fun.
  #
  # TODO: Prepare an SSCCE and report to https://github.com/pma/amqp/issues or
  # be a little bit autistic and use Erlang's amqp_client lib directly.
  @impl true
  def consume(chan, queue) do
    try_result =
      fn -> try_get_status(chan, queue) end
      |> Task.async()
      |> Task.await()

    with :ok <- try_result do
      AMQP.Basic.consume(chan, queue)
    end
  end

  @impl true
  defdelegate nack(chan, delivery_tag, options), to: AMQP.Basic

  @impl true
  def open_channel(conn, qos_opts) do
    with {:ok, chan} <- AMQP.Channel.open(conn),
         :ok <- AMQP.Basic.qos(chan, qos_opts) do
      {:ok, chan}
    else
      {:error, reason} -> {:error, reason}
      error -> {:error, error}
    end
  end

  @impl true
  defdelegate open_connection(options, connection_name), to: AMQP.Connection, as: :open

  @impl true
  defdelegate publish(chan, exchange, routing_key, payload, opts), to: AMQP.Basic

  # HACK: Tries to get the status of a queue in a isolated process and channel.
  # Note that both, the process and channel can crash, so better wrap this into
  # another monitored process.
  @spec try_get_status(AMQP.Channel.t(), AMQP.Basic.queue()) ::
          :ok | {:error, any()} | no_return()
  defp try_get_status(%AMQP.Channel{conn: conn}, queue) do
    caller = self()

    {pid, ref} = Process.spawn(fn -> do_try_get_status(caller, conn, queue) end, [:monitor])

    receive do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      {:DOWN, ^ref, :process, ^pid, reason} -> {:error, reason}
    after
      5_000 -> {:error, :timeout}
    end
  end

  @spec do_try_get_status(pid(), AMQP.Connection.t(), AMQP.Basic.queue()) :: :ok | no_return()
  defp do_try_get_status(caller, conn, queue) do
    case AMQP.Channel.open(conn) do
      {:error, reason} ->
        send(caller, {:error, reason})

      {:ok, chan} ->
        case AMQP.Queue.status(chan, queue) do
          {:ok, _status} -> send(caller, :ok)
          {:error, reason} -> send(caller, {:error, reason})
        end

        AMQP.Channel.close(chan)
    end

    :ok
  end
end

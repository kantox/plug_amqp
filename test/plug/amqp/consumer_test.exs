Code.require_file("../../support/trace.exs", __DIR__)

defmodule Plug.AMQP.ConsumerTest do
  use ExUnit.Case

  import Mox
  import Support.Trace

  alias Plug.AMQP.Backends.AMQP, as: Backend
  alias Plug.AMQP.Consumer

  setup :verify_on_exit!
  setup :set_mox_from_context

  describe "connection" do
    setup do
      stub(AMQPMock, :open_channel, fn _conn, _opts -> {:error, :not_now} end)
      stub(AMQPMock, :close_connection, &Backend.close_connection/1)

      :ok
    end

    test "must be open for consuming" do
      expect(AMQPMock, :open_connection, 1, trace(Backend, :open_connection, 2))

      pid = start_consumer!()

      assert_receive {:called, :open_connection, _result}
      assert Process.alive?(pid)
    end

    test "cause the consumer stop if connection is shutdown remotely" do
      expect(AMQPMock, :open_connection, 1, trace(Backend, :open_connection, 2))

      pid = start_consumer!()
      ref = Process.monitor(pid)

      assert_receive {:called, :open_connection, {:ok, conn}}

      Backend.close_connection(conn)

      assert_receive {:DOWN, ^ref, :process, ^pid,
                      {:connection_lost, :consumer, {:shutdown, :normal}}}
    end

    test "are retried several times on failures" do
      expect(AMQPMock, :open_connection, 2, fn _opts, _name -> {:error, :not_now} end)
      expect(AMQPMock, :open_connection, 1, trace(Backend, :open_connection, 2))

      start_consumer!(restart: :temporary)

      assert_receive {:called, :open_connection, _conn}
    end
  end

  describe "channel" do
    setup do
      stub(AMQPMock, :open_connection, &Backend.open_connection/2)
      stub(AMQPMock, :consume, fn _chan, _queue -> {:error, :not_now} end)

      :ok
    end

    test "must be open" do
      expect(AMQPMock, :open_channel, 1, trace(Backend, :open_channel, 2))

      pid = start_consumer!()

      assert_receive {:called, :open_channel, _result}
      assert Process.alive?(pid)
    end

    test "remote closes does not any effect on the connection" do
      expect(AMQPMock, :open_channel, 1, trace(Backend, :open_channel, 2))

      pid = start_consumer!(restart: :temporary)

      assert_receive {:calling, :open_channel, conn, []}
      assert_receive {:called, :open_channel, {:ok, chan}}

      expect(AMQPMock, :open_channel, 1, trace(Backend, :open_channel, 2))
      Backend.close_channel(chan)

      assert_receive {:calling, :open_channel, ^conn, []}

      assert Process.alive?(pid)
    end

    test "are retried several times on failures" do
      expect(AMQPMock, :open_channel, 2, fn _opts, _name -> {:error, :not_now} end)
      expect(AMQPMock, :open_channel, 1, trace(Backend, :open_channel, 2))

      start_consumer!(restart: :temporary)

      assert_receive {:called, :open_channel, _result}
    end
  end

  describe "consuming" do
    setup do
      stub(AMQPMock, :open_connection, &Backend.open_connection/2)
      stub(AMQPMock, :open_channel, &Backend.open_channel/2)

      :ok
    end

    test "is retried if the queue is not available at the beginning" do
      delete_consumer_queue()
      expect(AMQPMock, :consume, trace(Backend, :consume, 2))

      pid = start_consumer!(restart: :temporary)

      assert_receive {:called, :consume, _ctag}

      expect(AMQPMock, :consume, trace(Backend, :consume, 2, blocking: true))

      assert_receive {:calling, :consume, _channel, _queue}

      create_consumer_queue()

      send(pid, :continue)

      assert_receive {:called, :consume, _ctag}
      assert Process.alive?(pid)
    end

    test "is retried if the queue misbehave" do
      create_consumer_queue()
      expect(AMQPMock, :consume, trace(Backend, :consume, 2))

      pid = start_consumer!(restart: :temporary)

      assert_receive {:called, :consume, _ctag}

      expect(AMQPMock, :consume, trace(Backend, :consume, 2))

      delete_consumer_queue()

      assert_receive {:called, :consume, _ctag}
      assert Process.alive?(pid)
    end
  end

  describe "request handlers" do
    setup do
      stub_with(AMQPMock, Backend)

      create_consumer_queue()

      :ok
    end

    test "receive and acknowledge requests" do
      expect(AMQPMock, :ack, trace(Backend, :ack, 2))

      message_handler = trace(:message_handler, fn _server, _payload, _headers -> :ok end)

      pid = start_consumer!(message_handler: message_handler, restart: :temporary)

      send_message("", queue(), "foo", headers: [{"bar", :longstr, "qux"}])

      assert_receive {:calling, :message_handler, ^pid, "foo", [{"bar", :longstr, "qux"}]}
      assert_receive {:called, :message_handler, :ok}
      assert_receive {:calling, :ack, _chan, _delivery_tag}
      assert message_count() == 0
    end

    test "can raise safely when supervised" do
      supervisor = start_supervised!(Task.Supervisor)

      message_handler =
        trace(:message_handler, fn _server, _payload, _headers -> raise "to the moon" end)

      pid =
        start_consumer!(
          message_handler_supervisor: supervisor,
          message_handler: message_handler,
          restart: :temporary
        )

      send_message("", queue(), "payload", headers: [{"level", :short, 9001}])

      assert_receive {:calling, :message_handler, ^pid, "payload", [{"level", :short, 9001}]}
      assert Process.alive?(pid)
    end

    test "can return error responses, non acknowledging the request" do
      expect(AMQPMock, :nack, 2, trace(Backend, :nack, 3))

      queue = queue()

      send_message("", queue, "payload")

      message_handler = trace(:message_handler, fn _server, _payload, _headers -> :wtf end)
      pid = start_consumer!(message_handler: message_handler, restart: :temporary)

      assert_receive {:calling, :message_handler, ^pid, "payload", :undefined}

      assert_receive {:called, :message_handler, :wtf}
      assert_receive {:calling, :nack, _chan, _delivery_tag, _headers}
      assert_receive {:calling, :nack, _chan, _delivery_tag, requeue: false}
      assert message_count() == 0
    end

    test "acknowledge requests when handlers provides no response" do
      expect(AMQPMock, :ack, trace(Backend, :ack, 2))

      message_handler = trace(:message_handler, fn _server, _payload, _headers -> :ok end)
      start_consumer!(message_handler: message_handler, restart: :temporary)

      send_message("", queue(), "foo")

      assert_receive {:called, :message_handler, :ok}
      assert_receive {:calling, :ack, _chan, _delivery_tag}
    end
  end

  #
  # Helpers
  #

  @consumer_options_keys [:message_handler, :message_handler_supervisor]

  defp start_consumer!(opts \\ []) do
    {custom_consumer_options, supervisor_options} = Keyword.split(opts, @consumer_options_keys)

    consumer_producer = {Consumer, consumer_producer_options(custom_consumer_options)}

    start_supervised!(consumer_producer, supervisor_options)
  end

  defp create_consumer_queue do
    delete_consumer_queue()
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)
    {:ok, _info} = AMQP.Queue.declare(chan, queue())
    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)
  end

  defp delete_consumer_queue do
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)
    AMQP.Queue.delete(chan, queue())
    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)
  end

  def send_message(exchange, routing_key, payload, meta \\ []) do
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)
    :ok = AMQP.Basic.publish(chan, exchange, routing_key, payload, meta)
    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)
  end

  def message_count do
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)
    result = AMQP.Queue.message_count(chan, queue())
    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)
    result
  end

  #
  # Config
  #

  defp amqp_port do
    System.get_env("AMQP_PORT", "5672") |> String.to_integer()
  end

  defp connection_options do
    [port: amqp_port()]
  end

  defp queue do
    System.get_env("CONSUMER_QUEUE", "plug_amqp.integration")
  end

  defp consumer_producer_options(opts) do
    default_opts = [
      amqp_backend: AMQPMock,
      backoff: [kind: :exp, min: 1, max: 1],
      connection_options: connection_options(),
      queue: queue()
    ]

    Keyword.merge(default_opts, opts)
  end
end

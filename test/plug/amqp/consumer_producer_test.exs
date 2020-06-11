Code.require_file("../../support/trace.exs", __DIR__)

defmodule Plug.AMQP.ConsumerProducerTest do
  use ExUnit.Case

  import Mox
  import Support.Trace
  import ExUnit.CaptureLog

  alias Plug.AMQP.Backends.AMQP, as: Backend
  alias Plug.AMQP.ConsumerProducer

  setup :verify_on_exit!
  setup :set_mox_from_context

  @moduletag :integration

  describe "connections" do
    setup do
      stub(AMQPMock, :open_channel, fn _conn, _opts -> {:error, :not_now} end)
      stub(AMQPMock, :close_connection, &Backend.close_connection/1)

      :ok
    end

    test "must be open (one for consuming and one for producing)" do
      expect(AMQPMock, :open_connection, 2, trace(Backend, :open_connection, 2))

      pid = start_consumer_producer!()

      assert_receive {:called, :open_connection, _result}
      assert_receive {:called, :open_connection, _result}
      assert Process.alive?(pid)
    end

    test "are closed if consumer connection is shutdown remotely" do
      expect(AMQPMock, :open_connection, 2, trace(Backend, :open_connection, 2))

      start_consumer_producer!(restart: :temporary)

      assert_receive {:called, :open_connection, {:ok, consumer_conn_1}}
      assert_receive {:called, :open_connection, {:ok, producer_conn_1}}

      expect(AMQPMock, :close_connection, trace(Backend, :close_connection, 1))
      Backend.close_connection(consumer_conn_1)

      assert_receive {:calling, :close_connection, ^producer_conn_1}
    end

    test "are closed if producer connection is shutdown remotely" do
      expect(AMQPMock, :open_connection, 2, trace(Backend, :open_connection, 2))

      start_consumer_producer!(restart: :temporary)

      assert_receive {:called, :open_connection, {:ok, consumer_conn_1}}
      assert_receive {:called, :open_connection, {:ok, producer_conn_1}}

      expect(AMQPMock, :close_connection, trace(Backend, :close_connection, 1))
      Backend.close_connection(producer_conn_1)

      assert_receive {:calling, :close_connection, ^consumer_conn_1}
    end

    test "cause the consumer_producer to stop if consumer connection is shutdown remotely" do
      expect(AMQPMock, :open_connection, 2, trace(Backend, :open_connection, 2))

      pid = start_consumer_producer!()
      ref = Process.monitor(pid)

      assert_receive {:called, :open_connection, {:ok, consumer_conn_1}}
      assert_receive {:called, :open_connection, _result}

      Backend.close_connection(consumer_conn_1)

      assert_receive {:DOWN, ^ref, :process, ^pid,
                      {:connection_lost, :consumer, {:shutdown, :normal}}}
    end

    test "cause the consumer_producer to stop if producer connection is shutdown remotely" do
      expect(AMQPMock, :open_connection, 2, trace(Backend, :open_connection, 2))

      pid = start_consumer_producer!()
      ref = Process.monitor(pid)

      assert_receive {:called, :open_connection, _consumer_conn_1}
      assert_receive {:called, :open_connection, {:ok, producer_conn_1}}

      Backend.close_connection(producer_conn_1)

      assert_receive {:DOWN, ^ref, :process, ^pid,
                      {:connection_lost, :producer, {:shutdown, :normal}}}
    end

    test "are retried several times on failures" do
      expect(AMQPMock, :open_connection, 3, fn _opts, _name -> {:error, :not_now} end)
      expect(AMQPMock, :open_connection, 3, trace(Backend, :open_connection, 2))

      start_consumer_producer!(restart: :temporary)

      assert_receive {:called, :open_connection, _conn}
      assert_receive {:called, :open_connection, _conn}
      assert_receive {:called, :open_connection, _conn}
    end

    ## test "TODO - logs on error" do
    ## end
  end

  describe "channels" do
    setup do
      stub(AMQPMock, :open_connection, &Backend.open_connection/2)
      stub(AMQPMock, :consume, fn _chan, _queue -> {:error, :not_now} end)

      :ok
    end

    test "must be open (one for consuming and one for producing)" do
      expect(AMQPMock, :open_channel, 2, trace(Backend, :open_channel, 2))

      pid = start_consumer_producer!()

      assert_receive {:called, :open_channel, _result}
      assert_receive {:called, :open_channel, _result}
      assert Process.alive?(pid)
    end

    test "are closed if consumer channel is closed, but connections and process remain" do
      expect(AMQPMock, :open_channel, 2, trace(Backend, :open_channel, 2))

      pid = start_consumer_producer!(restart: :temporary)

      assert_receive {:called, :open_channel, {:ok, consumer_chan_1}}
      assert_receive {:called, :open_channel, {:ok, producer_chan_1}}

      expect(AMQPMock, :close_channel, trace(Backend, :close_channel, 1))
      expect(AMQPMock, :open_channel, 2, trace(Backend, :open_channel, 2))
      Backend.close_channel(consumer_chan_1)

      assert_receive {:calling, :close_channel, ^producer_chan_1}
      assert_receive {:called, :open_channel, {:ok, consumer_chan_2}}
      assert_receive {:called, :open_channel, {:ok, producer_chan_2}}

      assert consumer_chan_2.conn == consumer_chan_1.conn
      assert producer_chan_2.conn == producer_chan_1.conn
      assert Process.alive?(pid)
    end

    test "are closed if producer channel is closed, but connections and process remain" do
      expect(AMQPMock, :open_channel, 2, trace(Backend, :open_channel, 2))

      pid = start_consumer_producer!(restart: :temporary)

      assert_receive {:called, :open_channel, {:ok, consumer_chan_1}}
      assert_receive {:called, :open_channel, {:ok, producer_chan_1}}

      expect(AMQPMock, :close_channel, trace(Backend, :close_channel, 1))
      expect(AMQPMock, :open_channel, 2, trace(Backend, :open_channel, 2))
      Backend.close_channel(producer_chan_1)

      assert_receive {:calling, :close_channel, ^consumer_chan_1}
      assert_receive {:called, :open_channel, {:ok, consumer_chan_2}}
      assert_receive {:called, :open_channel, {:ok, producer_chan_2}}

      assert consumer_chan_2.conn == consumer_chan_1.conn
      assert producer_chan_2.conn == producer_chan_1.conn
      assert Process.alive?(pid)
    end

    test "are retried several times on failures" do
      expect(AMQPMock, :open_channel, 3, fn _opts, _name -> {:error, :not_now} end)
      expect(AMQPMock, :open_channel, 3, trace(Backend, :open_channel, 2))

      start_consumer_producer!(restart: :temporary)

      assert_receive {:called, :open_channel, _result}
      assert_receive {:called, :open_channel, _result}
      assert_receive {:called, :open_channel, _result}
    end

    ## test "TODO - Pending tasks are removed" do
    ## end

    ## test "TODO - Pending tasks are removed" do
    ## end

    ## test "TODO - logs on error" do
    ## end
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

      pid = start_consumer_producer!(restart: :temporary)

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

      pid = start_consumer_producer!(restart: :temporary)

      assert_receive {:called, :consume, _ctag}

      expect(AMQPMock, :consume, trace(Backend, :consume, 2))

      delete_consumer_queue()

      assert_receive {:called, :consume, _ctag}
      assert Process.alive?(pid)
    end

    # test "TODO - logs on error" do
    # end
  end

  describe "request handlers" do
    @argumnets_set [
      {"longstr", :longstr, "foo"},
      {"signedint", :signedint, 42},
      {"double", :double, 2.71828}
    ]

    @headers_set [
      {"longstr", "foo"},
      {"signedint", 42},
      {"double", 2.71828}
    ]

    setup do
      stub_with(AMQPMock, Backend)

      create_consumer_queue()

      :ok
    end

    test "receive and acknowledge requests" do
      expect(AMQPMock, :ack, trace(Backend, :ack, 2))

      request_handler = trace(:request_handler, fn _server, _payload, _headers -> :ok end)

      pid = start_consumer_producer!(request_handler: request_handler, restart: :temporary)

      send_request("", consumer_queue(), "foo", headers: [{"bar", :longstr, "qux"}])

      headers = [{"bar", "qux"}, {"amqp-routing-key", consumer_queue()}]
      assert_receive {:calling, :request_handler, ^pid, "foo", ^headers}
      assert_receive {:called, :request_handler, :ok}
      assert_receive {:calling, :ack, _chan, _delivery_tag}
      assert message_count() == 0
    end

    test "receive headers transformed to Erlang terms" do
      request_handler = trace(:request_handler, fn _server, _payload, _headers -> :ok end)

      pid = start_consumer_producer!(request_handler: request_handler, restart: :temporary)

      send_request("", consumer_queue(), "payload", headers: @argumnets_set)

      headers = @headers_set ++ [{"amqp-routing-key", consumer_queue()}]
      assert_receive {:calling, :request_handler, ^pid, "payload", ^headers}
    end

    test "can raise safely when supervised" do
      supervisor = start_supervised!(Task.Supervisor)

      request_handler =
        trace(:request_handler, fn _server, _payload, _headers -> raise "to the moon" end)

      pid =
        start_consumer_producer!(
          request_handler_supervisor: supervisor,
          request_handler: request_handler,
          restart: :temporary
        )

      send_request("", consumer_queue(), "payload", headers: [{"level", :short, 9001}])

      headers = [{"level", 9001}, {"amqp-routing-key", consumer_queue()}]
      assert_receive {:calling, :request_handler, ^pid, "payload", ^headers}
      assert Process.alive?(pid)
    end

    test "writes a log about requests without reply-to meta" do
      request_handler =
        trace(:request_handler, fn server, _payload, _headers ->
          ConsumerProducer.send_resp(server, "bar", [])

          :ok
        end)

      pid = start_consumer_producer!(request_handler: request_handler, restart: :temporary)
      send_request("", consumer_queue(), "payload")

      logs =
        capture_log(fn ->
          send_request("", consumer_queue(), "payload")
          assert_receive {:called, :request_handler, :ok}
          # FIXME: find another way to check this
          Process.sleep(1_000)
        end)

      assert logs =~ "expects no response"
      assert Process.alive?(pid)
    end

    test "can return error responses, non acknowledging the request" do
      expect(AMQPMock, :nack, 2, trace(Backend, :nack, 3))

      consumer_queue = consumer_queue()

      send_request("", consumer_queue, "payload")

      request_handler = trace(:request_handler, fn _server, _payload, _headers -> :wtf end)
      pid = start_consumer_producer!(request_handler: request_handler, restart: :temporary)

      assert_receive {:calling, :request_handler, ^pid, "payload",
                      [{"amqp-routing-key", ^consumer_queue}]}

      assert_receive {:called, :request_handler, :wtf}
      assert_receive {:calling, :nack, _chan, _delivery_tag, _headers}
      assert_receive {:calling, :nack, _chan, _delivery_tag, requeue: false}
      assert message_count() == 0
    end

    test "acknowledge requests when handlers provides no response" do
      expect(AMQPMock, :ack, trace(Backend, :ack, 2))

      request_handler = trace(:request_handler, fn _server, _payload, _headers -> :ok end)
      start_consumer_producer!(request_handler: request_handler, restart: :temporary)

      send_direct_reply_request("", consumer_queue(), "foo", correlation_id: "foo-id")

      assert_receive {:called, :request_handler, :ok}
      assert_receive {:calling, :ack, _chan, _delivery_tag}
    end

    test "response is published honoring reply_to" do
      request_handler =
        trace(:request_handler, fn server, payload, headers ->
          assert payload == "foo"

          ConsumerProducer.send_resp(server, "bar", headers)

          :ok
        end)

      start_consumer_producer!(request_handler: request_handler, restart: :temporary)

      {_conn, _chan, ctag} =
        send_direct_reply_request("", consumer_queue(), "foo",
          correlation_id: "foo-id",
          headers: @argumnets_set
        )

      headers =
        @argumnets_set ++
          [
            {"amqp-routing-key", :longstr, consumer_queue()},
            {"amqp-message-id", :longstr, "foo-id"}
          ]

      assert_receive {:basic_deliver, "bar", response_meta}
      assert response_meta.consumer_tag == ctag
      assert response_meta.correlation_id == "foo-id"
      assert Enum.sort(response_meta.headers) == Enum.sort(headers)
    end
  end

  #
  # Helpers
  #

  @consumer_producer_options_keys [:request_handler, :request_handler_supervisor]

  defp start_consumer_producer!(opts \\ []) do
    {custom_consumer_producer_options, supervisor_options} =
      Keyword.split(opts, @consumer_producer_options_keys)

    consumer_producer =
      {ConsumerProducer, consumer_producer_options(custom_consumer_producer_options)}

    start_supervised!(consumer_producer, supervisor_options)
  end

  defp create_consumer_queue() do
    delete_consumer_queue()
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)
    {:ok, _info} = AMQP.Queue.declare(chan, consumer_queue())
    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)
  end

  defp delete_consumer_queue() do
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)
    AMQP.Queue.delete(chan, consumer_queue())
    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)
  end

  def send_request(exchange, routing_key, payload, meta \\ []) do
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)
    :ok = AMQP.Basic.publish(chan, exchange, routing_key, payload, meta)
    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)
  end

  def send_direct_reply_request(exchange, routing_key, payload, meta \\ []) do
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)

    {:ok, ctag} = AMQP.Basic.consume(chan, "amq.rabbitmq.reply-to", nil, no_ack: true)

    assert_receive {:basic_consume_ok, %{consumer_tag: ^ctag}}

    request_meta =
      meta
      |> Keyword.put(:reply_to, "amq.rabbitmq.reply-to")
      |> Keyword.put(:message_id, "foo-id")

    :ok = AMQP.Basic.publish(chan, exchange, routing_key, payload, request_meta)

    {conn, chan, ctag}
  end

  def message_count() do
    {:ok, conn} = AMQP.Connection.open(connection_options())
    {:ok, chan} = AMQP.Channel.open(conn)
    result = AMQP.Queue.message_count(chan, consumer_queue())
    AMQP.Channel.close(chan)
    AMQP.Connection.close(conn)
    result
  end

  #
  # Config
  #

  defp amqp_port() do
    System.get_env("AMQP_PORT", "5672") |> String.to_integer()
  end

  defp connection_options() do
    [port: amqp_port()]
  end

  defp consumer_queue() do
    System.get_env("CONSUMER_QUEUE", "plug_amqp.integration")
  end

  defp consumer_producer_options(opts) do
    default_opts = [
      amqp_backend: AMQPMock,
      backoff: [kind: :exp, min: 1, max: 1],
      connection_options: connection_options(),
      consumer_queue: consumer_queue()
    ]

    Keyword.merge(default_opts, opts)
  end
end

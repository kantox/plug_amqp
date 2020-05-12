defmodule Plug.AMQPTest do
  use ExUnit.Case, async: true

  defmodule MyPlug do
    def call(conn = %Plug.Conn{path_info: ["foo", "bar"]}, _) do
      Plug.Conn.resp(conn, 200, "pong")
    end

    def call(conn = %Plug.Conn{path_info: ["force", "bar"]}, _) do
      conn
      |> Plug.Conn.resp(200, "pong")
      |> Plug.Conn.send_resp()
    end

    # simulate some error handler that writes response
    def call(conn, _) do
      conn
      |> Plug.Conn.resp(200, "ERROR")
      |> Plug.Conn.send_resp()

      raise "something"
    end
  end

  test "handle routed message" do
    assert :ok = Plug.AMQP.handle(self(), "ping", [{"amqp-routing-key", "foo.bar"}], plug: MyPlug)

    assert_receive {:send_resp, _, "pong", _}
  end

  test "handle routed message, force send response" do
    assert :ok =
             Plug.AMQP.handle(self(), "ping", [{"amqp-routing-key", "force.bar"}], plug: MyPlug)

    assert_receive {:send_resp, _, "pong", _}
  end

  test "handle message that would raise but still sends a resp" do
    Task.start(Plug.AMQP, :handle, [
      self(),
      "ping",
      [{"amqp-routing-key", "does.not.exist"}],
      [plug: MyPlug]
    ])

    assert_receive {:send_resp, _, "ERROR", _}
  end
end

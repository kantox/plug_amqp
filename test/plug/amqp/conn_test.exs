defmodule Plug.AMQP.ConnTest do
  use ExUnit.Case, async: true

  alias Plug.AMQP.UnsupportedError
  alias Plug.Conn

  defmodule MyPlug do
    use Plug.Router

    plug(:match)
    plug(Plug.Parsers, parsers: [:json], json_decoder: Jason)
    plug(:dispatch)

    post "/foo/:bar_param/:baz_param" do
      Conn.resp(conn, 200, "This is POST")
    end

    get "/foo/:bar_param/:baz_param" do
      Conn.resp(conn, 200, "This is GET")
    end

    match _ do
      Conn.resp(conn, 200, "UNMATCHED")
    end
  end

  test "Conn's basic stuff" do
    conn =
      make_conn("This is the body", [
        {"amqp-routing-key", "foo.bar.baz"}
      ])

    assert {:ok, "This is the body", conn} = Conn.read_body(conn)
    assert "http://www.example.com:0/foo/bar/baz" = Conn.request_url(conn)
    assert :"AMQP-0-9-1" = Conn.get_http_protocol(conn)

    assert %Conn{}
  end

  test "params" do
    conn =
      make_conn(~s[{"message":"This is the body"}], [
        {"amqp-routing-key", "foo.bar.baz"},
        {"amqp-content-type", "application/something+json"}
      ])

    conn = MyPlug.call(conn, MyPlug.init([]))

    assert %{
             "bar_param" => "bar",
             "baz_param" => "baz",
             "message" => "This is the body"
           } = conn.params

    assert "This is POST" = conn.resp_body
  end

  test "method override" do
    conn =
      make_conn(~s[{"message":"This is the body"}], [
        {"amqp-routing-key", "foo.bar.baz"},
        {"x-method-override", "get"},
        {"amqp-content-type", "application/json"}
      ])

    conn = MyPlug.call(conn, MyPlug.init([]))

    assert "This is GET" = conn.resp_body

    assert %{
             "bar_param" => "bar",
             "baz_param" => "baz"
           } = conn.params

    # body is ignored for get requests
    assert not is_map_key(conn.params, "message")
  end

  test "header normalization" do
    conn =
      make_conn(~s[{"message":"This is the body"}], [
        {"amqp-routing-key", "foo.bar.baz"},
        {"something_very-strange_got--here", 42}
      ])

    conn = MyPlug.call(conn, MyPlug.init([]))

    assert {"something-very-strange-got-here", "42"} in conn.req_headers
  end

  test "send_file raises an UnsupportedError" do
    assert_raise UnsupportedError, fn ->
      Plug.AMQP.Conn.send_file(make_conn(), 200, [], "/dev/null", 0, 0)
    end
  end

  test "send_chunked raises an UnsupportedError" do
    assert_raise UnsupportedError, fn ->
      Plug.AMQP.Conn.send_chunked(make_conn(), 200, [])
    end
  end

  test "chunk raises an UnsupportedError" do
    assert_raise UnsupportedError, fn ->
      Plug.AMQP.Conn.chunk(make_conn(), "payload")
    end
  end

  test "get_peer_data returns a well-known constant" do
    conn = make_conn()
    assert Plug.AMQP.Conn.get_peer_data(conn) == %{address: {0, 0, 0, 0}, port: 0, ssl_cert: nil}
  end

  defp make_conn(payload \\ "", meta \\ []) do
    Plug.AMQP.Conn.conn(nil, payload, meta)
  end
end

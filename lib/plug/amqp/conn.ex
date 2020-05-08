defmodule Plug.AMQP.Conn do
  @moduledoc """
  Adapter for AMQP to Plug.Conn

  This adapter partially implements the Plug.Conn.Adapter behaviour, with the
  following caveats:

  * `c:Plug.Conn.Adapter.send_file/6`, `c:Plug.Conn.Adapter.send_chunked/3` and
    `c:Plug.Conn.Adapter.chunk/2` raise, because there is no such functionality
    in *AMQP*. Also `c:Plug.Conn.Adapter.push/3` and
    `c:Plug.Conn.Adapter.inform/3` are not supported.

  * `c:Plug.Conn.Adapter.read_req_body/2` ignores the options and always returns
    the whole message body.

  * `request_path` is taken from the *routing_key*, by replacing dots with
    slashes and prepending a slash. This will play nice with existing plugs
    that expect url-like paths.

  * `method` is `"POST"` by default. You can override this behaviour setting a
    custom method in the `x-method-override` header.
  """

  @behaviour Plug.Conn.Adapter

  @routing_key_header "amqp-routing-key"
  @method_override_header "x-method-override"

  alias Plug.AMQP.{ConsumerProducer, UnsupportedError}

  @spec conn(GenServer.server(), ConsumerProducer.payload(), ConsumerProducer.headers()) ::
          Plug.Conn.t()
  def conn(consumer_producer, payload, headers) do
    headers = Enum.map(headers, fn {k, v} -> {normalize_header_name(k), to_string(v)} end)

    {_, routing_key} = Enum.find(headers, {nil, ""}, &match?({@routing_key_header, _}, &1))
    {_, method} = Enum.find(headers, {nil, "POST"}, &match?({@method_override_header, _}, &1))

    # Renormalize some common headers that are used by usual plugs.
    common_headers =
      Enum.flat_map(headers, fn
        {"amqp-content-encoding", v} -> [{"content-encoding", v}]
        # Compatibility with Plug.Parsers
        {"amqp-content-type", v} -> [{"content-type", v}]
        # Compatibility with Plug.RequestId
        {"amqp-message-id", v} -> [{"x-request-id", v}]
        _ -> []
      end)

    %Plug.Conn{
      adapter: {__MODULE__, {consumer_producer, payload}},
      method: String.upcase(method),
      owner: self(),
      path_info: :binary.split(routing_key, ".", [:global, :trim_all]),
      remote_ip: {0, 0, 0, 0},
      req_headers: common_headers ++ headers,
      request_path: "/" <> :binary.replace(routing_key, ".", "/", [:global])
    }
  end

  defp normalize_header_name(name) do
    name |> to_string() |> String.downcase() |> String.replace(~r|[^a-zA-Z0-9]+|, "-")
  end

  @impl true
  def send_resp(req = {consumer_producer, _req_payload}, _status, headers, body) do
    ConsumerProducer.send_resp(consumer_producer, body, headers)
    {:ok, body, req}
  end

  @impl true
  def send_file(_req, _status, _headers, _path, _offset, _length) do
    raise UnsupportedError
  end

  @impl true
  def send_chunked(_req, _status, _headers) do
    raise UnsupportedError
  end

  @impl true
  def chunk(_req, _body) do
    raise UnsupportedError
  end

  @impl true
  def read_req_body(req = {_endpoint, payload}, _opts) do
    {:ok, payload, req}
  end

  @impl true
  def inform(_req, _status, _headers) do
    {:error, :not_supported}
  end

  @impl true
  def push(_req, _path, _headers) do
    {:error, :not_supported}
  end

  @impl true
  def get_peer_data(_conn) do
    %{address: {0, 0, 0, 0}, port: 0, ssl_cert: nil}
  end

  @impl true
  def get_http_protocol(_req) do
    :"AMQP-0-9-1"
  end
end

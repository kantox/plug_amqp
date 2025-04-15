defmodule Plug.AMQP.Conn do
  @moduledoc """
  TODO
  """

  @behaviour Plug.Conn.Adapter

  alias AMQPHelpers.Reliability.Producer
  alias Plug.AMQP.UnsupportedError

  @spec conn(pid(), binary(), map()) :: struct()
  def conn(producer, payload, meta) do
    headers =
      if is_list(meta.headers) do
        Enum.map(meta.headers, fn {key, _type, value} ->
          {String.downcase(key), to_string(value)}
        end)
      else
        []
      end

    host = get_amqp_header(headers, "x-plug-amqp-req-host", meta.consumer_tag)
    method = get_amqp_header(headers, "x-plug-amqp-req-method", "POST")
    query_string = get_amqp_header(headers, "x-plug-amqp-req-query", "")

    req_headers =
      headers
      |> Stream.reject(&match?({"x-plug-amqp-req-host", _type, _value}, &1))
      |> Stream.reject(&match?({"x-plug-amqp-req-method", _type, _value}, &1))
      |> Stream.reject(&match?({"x-plug-amqp-req-query", _type, _value}, &1))
      |> Stream.concat(
        case meta.content_encoding do
          :undefined ->
            []

          content_encoding ->
            [{"content-encoding", content_encoding}]
        end
      )
      |> Stream.concat(
        case meta.content_type do
          :undefined ->
            []

          content_type ->
            [{"content-type", content_type}]
        end
      )
      |> Enum.concat([{"content-length", "#{byte_size(payload)}"}])

    %Plug.Conn{
      adapter: {__MODULE__, {producer, payload, meta}},
      host: host,
      method: method,
      owner: self(),
      path_info: :binary.split(meta.routing_key, ".", [:global, :trim_all]),
      query_string: query_string,
      remote_ip: {0, 0, 0, 0},
      req_headers: req_headers,
      request_path: "/" <> :binary.replace(meta.routing_key, ".", "/", [:global])
    }
  end

  @impl true
  def chunk(_req, _body) do
    raise UnsupportedError
  end

  @impl true
  def get_http_protocol(_req) do
    :"AMQP-0-9-1"
  end

  @impl true
  def get_peer_data(_conn) do
    # TODO: Maybe return the IP of the broker? IP/ID of the caller?
    %{address: {0, 0, 0, 0}, port: 0, ssl_cert: nil}
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
  def read_req_body(req = {_producer, payload, _meta}, _opts) do
    {:ok, payload, req}
  end

  @impl true
  def send_chunked(_req, _status, _headers) do
    raise UnsupportedError
  end

  @impl true
  def send_file(_req, _status, _headers, _path, _offset, _length) do
    raise UnsupportedError
  end

  @impl true
  def send_resp(req = {_producer, _payload, %{reply_to: :undefined}}, _status, _headers, _body) do
    {:ok, nil, req}
  end

  # TODO: Add mandatory as an optional header
  # TODO: Add immediate as an optional header
  # TODO: Add priority as an optional header
  # TODO: Add reply_to as an optional header
  # TODO: Add expiration as an optional header
  # TODO: Add user_id as an optional header
  # TODO: Add app_id as an optional header
  def send_resp(req = {producer, _payload, meta}, status, headers, body) do
    content_type = get_plug_header(headers, "content-type")

    content_encoding = get_plug_header(headers, "content-encoding")

    message_headers =
      headers
      |> Stream.reject(&match?({"content-type", _value}, &1))
      |> Stream.reject(&match?({"content-encoding", _value}, &1))
      |> Stream.reject(&match?({"x-plug-amqp-resp-persistent", _value}, &1))
      |> Stream.reject(&match?({"x-plug-amqp-resp-message-id", _value}, &1))
      |> Stream.reject(&match?({"x-plug-amqp-resp-timestamp", _value}, &1))
      |> Stream.reject(&match?({"x-plug-amqp-resp-type", _value}, &1))
      |> Enum.map(fn {key, value} -> {key, :binary, value} end)
      |> then(&[{"x-plug-amqp-http-status", :long, status} | &1])

    persistent =
      case get_plug_header(headers, "x-plug-amqp-resp-persistent") do
        :undefined -> true
        "true" -> true
        "false" -> false
      end

    message_id = get_plug_header(headers, "x-plug-amqp-resp-message-id", Uniq.UUID.uuid7())

    timestamp =
      case get_plug_header(headers, "x-plug-amqp-resp-timestamp") do
        :undefined -> System.os_time(:second)
        timestamp_string -> String.to_integer(timestamp_string)
      end

    type = get_plug_header(headers, "x-plug-amqp-resp-type")

    # TODO
    publish_opts = [
      content_type: content_type,
      content_encoding: content_encoding,
      headers: message_headers,
      persistent: persistent,
      correlation_id: get_response_correlation_id(meta),
      message_id: message_id,
      timestamp: timestamp,
      type: type
    ]

    body = if is_list(body), do: IO.iodata_to_binary(body), else: body

    :ok = Producer.publish(producer, "", meta.reply_to, body, publish_opts)

    {:ok, nil, req}
  end

  @impl true
  def upgrade(_payload, _protocol, _opts) do
    {:error, :not_supported}
  end

  @spec get_amqp_header([{String.t(), String.t()}], String.t(), term()) :: term()
  defp get_amqp_header(headers, key, default) do
    case Enum.find(headers, &match?({^key, _value}, &1)) do
      {^key, _type, value} -> value
      _other -> default
    end
  end

  @spec get_response_correlation_id(map()) :: term()
  defp get_response_correlation_id(%{correlation_id: correlation_id})
       when correlation_id != :undefined do
    correlation_id
  end

  defp get_response_correlation_id(%{correlation_id: :undefined, message_id: message_id})
       when message_id != :undefined do
    message_id
  end

  defp get_response_correlation_id(_meta), do: :undefined

  @spec get_plug_header([{binary(), binary()}], binary(), binary() | :undefined) ::
          binary() | :undefined
  defp get_plug_header(headers, key, default \\ :undefined) do
    case Enum.find(headers, &match?({^key, _value}, &1)) do
      {^key, value} -> value
      _other -> default
    end
  end
end

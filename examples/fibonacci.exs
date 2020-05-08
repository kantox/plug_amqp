# The Server

defmodule MyPlug do
  @behaviour Plug

  @impl true
  def init(_opts), do: nil

  @impl true
  def call(conn, _opts) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)
    input = String.to_integer(body)

    output = fib(input)

    resp_body = to_string(output)
    Plug.Conn.send_resp(conn, 200, resp_body)
  end

  defp fib(0), do: 0
  defp fib(1), do: 1
  defp fib(n) when n > 1, do: fib(n - 1) + fib(n - 2)
end

# Setting Up the RPC Request Queue

{:ok, conn} = AMQP.Connection.open()
{:ok, chan} = AMQP.Channel.open(conn)

{:ok, _info} = AMQP.Queue.declare(chan, "rpc_queue")

# Starting the Server

{:ok, _adapter} = Plug.AMQP.start_link(consumer_queue: "rpc_queue", plug: MyPlug)

# Setting the Client

{:ok, %{queue: callback_queue}} = AMQP.Queue.declare(chan, "", exclusive: true)
{:ok, _ctag} = AMQP.Basic.consume(chan, callback_queue, nil, no_ack: true)

# Sending a Request

IO.puts("Sending a 30 as a request")
AMQP.Basic.publish(chan, "", "rpc_queue", "30", reply_to: callback_queue)

# Waiting a Response

receive do
  {:basic_deliver, response, _meta} ->
    IO.puts("Got a #{response} response")
end

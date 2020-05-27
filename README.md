# PlugAmqp

[![Hex Version](https://img.shields.io/hexpm/v/plug_amqp.svg?maxAge=3600)](https://hex.pm/packages/plug_amqp)
[![License](https://img.shields.io/hexpm/l/plug_amqp.svg?maxAge=3600)](https://hex.pm/packages/plug_amqp)
[![Build and Test](https://github.com/kantox/plug_amqp/workflows/Build%20and%20Test/badge.svg)](https://github.com/kantox/plug_amqp/actions)
[![Coverage Status](https://coveralls.io/repos/github/kantox/plug_amqp/badge.svg?t=2ISMwr)](https://coveralls.io/github/kantox/plug_amqp)

`Plug.AMQP` provides an [AMQP](https://www.amqp.org) interface to `Plug`. When
using `Plug.AMQP` we can write servers that answer requests sent through an
*AMQP* broker, like [RabbitMQ](https://www.rabbitmq.com). The request response
pattern is explained in detail
[here](https://www.rabbitmq.com/tutorials/tutorial-six-elixir.html).

[Online documentation](https://hexdocs.pm/plug_amqp/Plug.AMQP.html).

## Installation

We can use `plug_amqp` in our projects by adding the dependency:

```elixir
def deps do
  [
    {:plug_amqp, "~> 0.5"},
  ]
end
```

## Usage

To use `Plug.AMQP`, add it to the supervision tree:

```elixir
children = [
  {Plug.AMQP, connection_options: "amqp://my-rabbit:5672", plug: MyPlug}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Here it is a `Plug` as an example:

```elixir
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
```

Then we can start sending messages to the server through *AMQP*:

```elixir
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
```

Check `Plug.AMQP` module from the
[online documentation](https://hexdocs.pm/plug_amqp/Plug.AMQP.html) for more
information. Also, take a look the examples in the `examples` folder.

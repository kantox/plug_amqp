# Setting Up A Client

{:ok, conn} = AMQP.Connection.open()
{:ok, chan} = AMQP.Channel.open(conn)

{:ok, _info} = AMQP.Queue.declare(chan, "plug_amqp.consumer.requests")

# Setting Up Consumer Server

example_process = self()

{:ok, _server} =
  Plug.AMQP.Consumer.start_link(
    queue: "plug_amqp.consumer.requests",
    message_handler: fn server, request, _headers ->
      IO.puts("Got the message: \"#{request}\"")
      IO.puts("The message will be acknowledged after returning :ok")

      send(example_process, :proceed)

      :ok
    end
  )

# Sending A Message

IO.puts("Sending a message with payload \"foo\"")

AMQP.Basic.publish(chan, "", "plug_amqp.consumer.requests", "foo")

receive do
  :proceed -> :ok
end

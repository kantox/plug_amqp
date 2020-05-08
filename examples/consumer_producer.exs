# Setting Up A Client

{:ok, conn} = AMQP.Connection.open()
{:ok, chan} = AMQP.Channel.open(conn)

{:ok, _info} = AMQP.Queue.declare(chan, "plug_amqp.consumer_producer.requests")

{:ok, _ctag} = AMQP.Basic.consume(chan, "amq.rabbitmq.reply-to", nil, no_ack: true)

# Setting Up Consumer-Producer Server

{:ok, _server} =
  Plug.AMQP.ConsumerProducer.start_link(
    consumer_queue: "plug_amqp.consumer_producer.requests",
    request_handler: fn server, request, _headers ->
      number = String.to_integer(request)
      Plug.AMQP.ConsumerProducer.send_resp(server, to_string(number + 1))

      :ok
    end
  )

# Sending A Request

IO.puts("Sending a request with payload \"1\"")

AMQP.Basic.publish(chan, "", "plug_amqp.consumer_producer.requests", "1",
  reply_to: "amq.rabbitmq.reply-to"
)

# Receiving A Response

receive do
  {:basic_deliver, response, _meta} ->
    IO.puts("Received a response with payload \"#{response}\"")
end

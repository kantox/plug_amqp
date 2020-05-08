Mox.defmock(AMQPMock, for: Plug.AMQP.Backend)

ExUnit.configure(
  assert_receive_timeout: 6_000,
  exclude: [integration: true]
)

ExUnit.start()

defmodule Plug.AMQP.Backend do
  # This module is used as interface to the *AMQP* library. Useful for creating
  # mocks using Mox.
  @moduledoc false

  alias AMQP.{Basic, Channel, Connection}

  @type error() :: {:error, any()}

  @callback ack(Channel.t(), Basic.delivery_tag()) :: :ok | error()

  @callback close_channel(Channel.t()) :: :ok | error()

  @callback close_connection(Connection.t()) :: :ok | error()

  @callback consume(Channel.t(), Basic.queue()) :: {:ok, String.t()} | error()

  @callback nack(Channel.t(), Basic.delivery_tag(), keyword()) :: :ok | error()

  @callback open_channel(Connection.t(), qos_options :: keyword()) :: {:ok, Channel.t()} | error()

  @callback open_connection(
              uri_or_options :: String.t() | keyword(),
              name :: String.t() | :undefined
            ) :: {:ok, Connection.t()} | error()

  @callback publish(
              Channel.t(),
              Basic.exchange(),
              Basic.routing_key(),
              Basic.payload(),
              Keyword.t()
            ) :: :ok | error()
end

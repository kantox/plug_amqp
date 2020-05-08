# PlugAmqp

![Build and Test](https://github.com/kantox/plug_amqp/workflows/Build%20and%20Test/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/kantox/plug_amqp/badge.svg?t=DSnX7A)](https://coveralls.io/github/kantox/plug_amqp)

A Plug adapter for [AMQP](https://www.amqp.org/).

## Installation

You can use `plug_amqp` in your project by adding the dependency:

```elixir
def deps do
  [
    {:plug_amqp, "~> 0.5"},
  ]
end
```

You can then start the adapter with:

```elixir
Plug.AMQP, plug: MyPlug
```

Check `Plug.AMQP` module from the
[online documentation](https://kantox.hexdocs.pm/plug_amqp) for more
information. Also, check the code under the `examples` folder.

# PlugAmqp

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

Check `Plug.AMQP` module documentation for more information.

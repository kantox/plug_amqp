defmodule PlugAmqpTest do
  use ExUnit.Case
  doctest PlugAmqp

  test "greets the world" do
    assert PlugAmqp.hello() == :world
  end
end

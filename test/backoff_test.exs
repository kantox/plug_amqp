defmodule BackoffTest do
  use ExUnit.Case, async: true
  use PropCheck

  doctest Backoff

  alias Backoff

  property "backoffs start with a value greater or equal than min" do
    forall [kind <- kind(), min <- non_neg_integer()] do
      backoff = Backoff.new(kind: kind, min: min)
      {step_value, _next_backoff} = Backoff.step(backoff)

      assert step_value >= min
    end
  end

  property "backoffs increase until max" do
    forall [kind <- kind(), max <- non_neg_integer(), count <- pos_integer()] do
      backoff = Backoff.new(kind: kind, max: max)

      {step_value, _next_backoff} =
        Enum.reduce(1..count, {0, backoff}, fn _, {_, backoff} ->
          Backoff.step(backoff)
        end)

      assert step_value <= max
    end
  end

  property "reseting a newly created backoff returns the same backoff" do
    forall kind <- kind() do
      backoff = Backoff.new(kind: kind)

      assert Backoff.reset(backoff) === backoff
    end
  end

  # TODO: report this as issue to the original implementation
  # https://github.com/dashbitco/broadway_rabbitmq/issues
  property "exponential backoffs always increase until max value" do
    forall [kind <- exp_kind(), min <- non_neg_integer(), max <- non_neg_integer()] do
      implies min < max do
        backoff = Backoff.new(kind: kind, min: min, max: max)

        {first_value, next_backoff} = Backoff.step(backoff)
        {second_value, _next_backoff} = Backoff.step(next_backoff)

        assert second_value > first_value || second_value == max
      end
    end
  end

  property "inspect implementation shows kind, min and max values" do
    forall [kind <- exp_kind(), min <- non_neg_integer(), max <- non_neg_integer()] do
      implies min < max do
        backoff = Backoff.new(kind: kind, min: min, max: max)

        assert inspect(backoff) =~ to_string(kind)
        assert inspect(backoff) =~ to_string(min)
        assert inspect(backoff) =~ to_string(max)
      end
    end
  end

  defp exp_kind(), do: union([:exp, :rand_exp])
  defp kind(), do: union([:rand, :exp, :rand_exp])
end

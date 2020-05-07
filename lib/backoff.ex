defmodule Backoff do
  @moduledoc false

  # Functions to decrease the rate of some process.
  #
  # A **Backoff** algorithm is commonly used to space out repeated retransmissions
  # of the same block of data, avoiding congestion.
  #
  # This module provides a data structure, [Backoff](#t:t/0), that holds the state
  # and the configuration of the backoff algorithm. Then, we can use function
  # `step/1` to get the time to wait for repeating a process and a new state of
  # the backoff algorithm.
  #
  # ## Example
  #
  #    #iex> backoff = Backoff.new(kind: :exp)
  #    ##Backoff<kind: exp, min: 200, max: 15000>
  #    #iex> {200, next_backoff} = Backoff.step(backoff)
  #    #iex> {400, _next_backoff} = Backoff.step(next_backoff)

  use Bitwise, only_operators: true

  @default_kind :rand_exp
  @default_min 200
  @default_max 15_000

  @typedoc """
  The implementation used to provide the [Backoff](#t:t/0) behaviour.

  There are different ways to provide [Backoff](#t:t/0) behaviour:

  * `rand`: on every step, the delay time is computed randomly between
    two values, *min* and *max*.
  * `exp`: every step the delay is increased exponentially.
  * `rand_exp`: a combination of the previous two.
  """
  @type kind :: :rand | :exp | :rand_exp

  @typedoc ~s"""
  Available options to configure a [Backoff](#t:t/0).

  * `kind`: the implementation to be used. Can be any of the available
  `t:kind/0`s. Defaults to `#{@default_kind}`.
  * `min`: the minimum value that can return a *Backoff*. Defaults to
  #{@default_min}.
  * `max`: the maximum value that can return a *Backoff*. Defaults to
  #{@default_max}.
  """
  @type option() ::
          {:kind, kind()}
          | {:min, non_neg_integer()}
          | {:max, non_neg_integer()}

  @typedoc "A list of `t:option/0`."
  @type options() :: [option()]

  @typedoc """
  A *Backoff* state.

  An opaque data structure that holds the state and the configuration of a
  *Backoff* algorithm. Can be created with `new/0` or `new/1`.
  """
  @opaque t() :: %__MODULE__{
            kind: kind(),
            min: non_neg_integer(),
            max: non_neg_integer(),
            state: term()
          }
  defstruct [:kind, :min, :max, :state]

  @doc """
  Creates a new [Backoff](#t:t/0).

  Returns a new [Backoff](#t:t/0) configured by `t:options/0`.
  """
  @spec new() :: t()
  @spec new(options()) :: t()
  def new(opts \\ []) do
    kind = Keyword.get(opts, :kind, @default_kind)
    {min, max} = min_max(opts)

    do_new(kind, min, max)
  end

  @doc """
  Sets a [Backoff](#t:t/0) to the initial state.

  Given a [Backoff](#t:t/0), sets its state back to the initial value. Note that for
  `rand` implementation this functions has no effect.
  """
  @spec reset(t()) :: t()
  def reset(backoff = %__MODULE__{kind: :exp, min: min}), do: %{backoff | state: min}

  def reset(backoff = %__MODULE__{kind: :rand_exp, min: min, state: {_last, seed}}) do
    %__MODULE__{backoff | state: {min, seed}}
  end

  def reset(backoff = %__MODULE__{kind: :rand}), do: backoff

  @doc """
  Computes the current delay.

  Given a [Backoff](#t:t/0), returns the current delay time a next state of the
  current [Backoff](#t:t/0).
  """
  @spec step(t()) :: {non_neg_integer(), t()}
  def step(backoff = %__MODULE__{kind: :rand, min: min, max: max, state: seed}) do
    {diff, next_seed} = :rand.uniform_s(max - min + 1, seed)
    {diff + min - 1, %{backoff | state: next_seed}}
  end

  def step(backoff = %__MODULE__{kind: :exp, max: max, state: state}) do
    {state, %{backoff | state: min(max(state, 1) <<< 1, max)}}
  end

  def step(backoff = %__MODULE__{kind: :rand_exp, max: max, state: {last, seed}}) do
    upper_bound = max(last, 1) <<< 1
    {diff, next_seed} = :rand.uniform_s(upper_bound + 1, seed)
    next_value = min(last + diff, max)

    {next_value, %{backoff | state: {next_value, next_seed}}}
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%{kind: kind, min: min, max: max}, inspect_opts) do
      fields = [{"kind:", kind}, {"min:", min}, {"max:", max}]

      fields_doc =
        container_doc("<", fields, ">", inspect_opts, fn {key, value}, _opts ->
          glue(key, to_string(value))
        end)

      concat("#Backoff", fields_doc)
    end
  end

  @spec min_max(options()) :: {number(), number()}
  defp min_max(opts) do
    case {Keyword.get(opts, :min), Keyword.get(opts, :max)} do
      {nil, nil} -> {@default_min, @default_max}
      {nil, max} -> {min(@default_min, max), max}
      {min, nil} -> {min, max(min, @default_max)}
      {min, max} -> {min, max}
    end
  end

  @spec do_new(kind(), number(), number()) :: t() | no_return()
  defp do_new(_, min, _) when not (is_integer(min) and min >= 0) do
    raise ArgumentError, "minimum #{inspect(min)} not 0 or a positive integer"
  end

  defp do_new(_, _, max) when not (is_integer(max) and max >= 0) do
    raise ArgumentError, "maximum #{inspect(max)} not 0 or a positive integer"
  end

  defp do_new(_, min, max) when min > max do
    raise ArgumentError, "minimum #{min} is greater than maximum #{max}"
  end

  defp do_new(:rand, min, max) do
    seed = :rand.seed_s(:exsplus)
    %__MODULE__{kind: :rand, min: min, max: max, state: seed}
  end

  defp do_new(:exp, min, max) do
    %__MODULE__{kind: :exp, min: min, max: max, state: min}
  end

  defp do_new(:rand_exp, min, max) do
    seed = :rand.seed_s(:exsplus)
    %__MODULE__{kind: :rand_exp, min: min, max: max, state: {min, seed}}
  end

  defp do_new(kind, _min, _max) do
    raise ArgumentError, "unknown kind #{inspect(kind)}"
  end
end

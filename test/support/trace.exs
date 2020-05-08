defmodule Support.Trace do
  #
  # Trace Macro
  #
  # Allows tracing calls to a function from the current process. This is useful
  # for testing purposes. It also allow us to block the execution of a function.
  #
  # ## Examples
  #
  #     my_func = trace(:my_id, fn a -> a + 40 end)
  #     my_func(2)
  #     assert_receive {:calling, :my_id, 2}
  #     assert_receive {:called, :my_id, 42}
  #
  #     my_div = trace(:div, Kernel, :div, 2)
  #     my_div(10, 2)
  #     assert_receive {:calling, :div, 10, 2}
  #     assert_receive {:called, :div, 5}
  #
  #     a_blocking_call = trace(:foo, fn a -> a + 40 end, blocking: true)
  #     pid = Task.start(fn -> a_blocking_call(2) end)
  #     assert_receive {:calling, :div, 10, 2}
  #     send(pid, :continue)
  #     assert_receive {:called, :div, 5}
  #

  defmacro trace(module_or_function_name, function_name_or_body, arity_or_otps \\ [], opts \\ [])

  defmacro trace(module, fun, arity, opts) when is_atom(fun) and is_number(arity) do
    args = Macro.generate_arguments(arity, __MODULE__)
    call = quote(do: apply(unquote(module), unquote(fun), unquote(args)))

    do_trace(fun, call, args, opts)
  end

  defmacro trace(name, fun, opts, _) do
    {:fn, _fn_meta, [{:->, _arrow_meta, [fun_args | _body]}]} = fun
    arity = length(fun_args)
    args = Macro.generate_arguments(arity, __MODULE__)
    call = quote(do: unquote(fun).(unquote_splicing(args)))

    do_trace(name, call, args, opts)
  end

  defp do_trace(name, call, args, opts) do
    quote do
      caller_pid = self()

      fn unquote_splicing(args) ->
        Task.start(fn ->
          send(caller_pid, {:calling, unquote(name), unquote_splicing(args)})
        end)

        unquote(maybe_stop(opts))

        result = unquote(call)
        Task.start(fn -> send(caller_pid, {:called, unquote(name), result}) end)
        result
      end
    end
  end

  defp maybe_stop(opts) do
    if Keyword.get(opts, :blocking, false) do
      quote do
        receive do
          :continue -> :ok
        after
          5_000 ->
            raise RuntimeError, :timeout
        end
      end
    else
      quote(do: nil)
    end
  end
end

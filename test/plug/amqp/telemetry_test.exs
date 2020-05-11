defmodule Plug.AMQP.TelemetryTest do
  use ExUnit.Case, async: true

  def attach_telemetry() do
    unique_name = :"PID#{System.unique_integer()}"
    Process.register(self(), unique_name)

    for suffix <- [:start, :stop, :exception] do
      :telemetry.attach(
        {suffix, unique_name},
        [:plug_adapter, :call, suffix],
        fn event, measurements, metadata, :none ->
          send(unique_name, {:event, event, measurements, metadata})
        end,
        :none
      )
    end

    on_exit(fn ->
      for suffix <- [:start, :stop, :exception] do
        :telemetry.detach({suffix, unique_name})
      end
    end)
  end

  defmodule MyPlug do
    def call(conn = %{path_info: ["telemetry_stop"]}, _) do
      Plug.Conn.send_resp(conn, 200, "STOP")
    end

    def call(conn = %{path_info: ["telemetry_exception"]}, _) do
      Plug.Conn.send_resp(conn, 200, "STOP")
      raise "oops"
    end
  end

  test "emits telemetry events for start/stop" do
    attach_telemetry()

    Plug.AMQP
    |> Task.async(:handle, [
      self(),
      "ping",
      [{"amqp-routing-key", "telemetry_stop"}],
      [plug: MyPlug]
    ])

    assert_receive {:event, [:plug_adapter, :call, :start], %{system_time: _},
                    %{
                      adapter: :plug_amqp,
                      conn: %{request_path: "/telemetry_stop"},
                      plug: MyPlug
                    }}

    assert_receive {:send_resp, _, "STOP", _}

    assert_receive {:event, [:plug_adapter, :call, :stop], %{duration: _},
                    %{
                      adapter: :plug_amqp,
                      conn: %{request_path: "/telemetry_stop", status: 200},
                      plug: MyPlug
                    }}

    refute_received {:event, [:plug_adapter, :call, :exception], _, _}
  end

  test "emits telemetry events for start/exception" do
    attach_telemetry()

    Plug.AMQP
    |> Task.start(:handle, [
      self(),
      "ping",
      [{"amqp-routing-key", "telemetry_exception"}],
      [plug: MyPlug]
    ])

    assert_receive {:event, [:plug_adapter, :call, :start], %{system_time: _},
                    %{
                      adapter: :plug_amqp,
                      conn: %{request_path: "/telemetry_exception"},
                      plug: MyPlug
                    }}

    assert_receive {:event, [:plug_adapter, :call, :exception], %{duration: _},
                    %{
                      adapter: :plug_amqp,
                      reason: %RuntimeError{},
                      conn: %{request_path: "/telemetry_exception"},
                      plug: MyPlug
                    }}

    refute_received {:event, [:plug_adapter, :call, :stop], _, _}
  end
end

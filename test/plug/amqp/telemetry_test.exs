defmodule Plug.AMQP.TelemetryTest do
  use ExUnit.Case, async: true

  def attach_telemetry do
    this = self()

    :telemetry.attach_many(
      this,
      Enum.map([:start, :stop, :exception], &[:plug_adapter, :call, &1]),
      fn event, measurements, metadata, :none ->
        send(this, {:event, event, measurements, metadata})
      end,
      :none
    )

    on_exit(fn -> :telemetry.detach(this) end)
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

    Task.async(Plug.AMQP, :handle, [
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

    refute_received {:event, [:plug_adapter, :call, :exception], _, %{plug: MyPlug}}
  end

  test "emits telemetry events for start/exception" do
    attach_telemetry()

    Task.start(Plug.AMQP, :handle, [
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

    refute_received {:event, [:plug_adapter, :call, :stop], _, %{plug: MyPlug}}
  end
end

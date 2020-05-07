defmodule PlugAmqp.MixProject do
  use Mix.Project

  def project do
    [
      app: :plug_amqp,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: test_coverage(),
      preferred_cli_env: preferred_cli_env()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:amqp, "~> 1.4"},
      {:ex_doc, "~> 0.12", only: :dev, runtime: false},
      {:excoveralls, "~> 0.12", only: :test},
      {:propcheck, "~> 1.2", only: :test},
      {:telemetry, "~> 0.4"}
    ]
  end

  defp test_coverage do
    [
      tool: ExCoveralls
    ]
  end

  defp preferred_cli_env do
    [
      coveralls: :test,
      "coveralls.detail": :test,
      "coveralls.github": :test,
      "coveralls.html": :test,
      "coveralls.post": :test
    ]
  end
end

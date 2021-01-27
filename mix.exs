defmodule PlugAmqp.MixProject do
  use Mix.Project

  @version "0.6.0"
  @description "A Plug adapter for AMQP"

  def project do
    [
      app: :plug_amqp,
      version: @version,
      elixir: "~> 1.10",
      deps: deps(),
      description: @description,
      name: "PlugAMQP",
      source_url: "https://github.com/kantox/plug_amqp",
      homepage_url: "https://github.com/kantox/plug_amqp",
      docs: docs(),
      package: package(),
      dialyzer: dialyzer(),
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
      {:amqp, "~> 2.0"},
      {:credo, "~> 1.2", only: :dev, runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.12", only: :dev, runtime: false},
      {:excoveralls, "~> 0.12", only: :test},
      {:mox, "~> 1.0", only: :test},
      {:plug, "~> 1.7"},
      {:propcheck, "~> 1.2", only: :test},
      {:telemetry, "~> 0.4"}
    ]
  end

  defp docs do
    [main: "Plug.AMQP", extras: ["README.md"]]
  end

  defp package do
    [
      files: ~w(lib examples mix.exs README.md .formatter.exs),
      licenses: ["MIT"],
      links: %{github: "https://github.com/kantox/plug_amqp"}
    ]
  end

  defp dialyzer do
    [
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
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

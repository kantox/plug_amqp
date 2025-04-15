defmodule PlugAmqp.MixProject do
  use Mix.Project

  def project do
    [
      app: :plug_amqp,
      version: "2.0.2",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:amqp, "~> 3.0"},
      {:amqp_helpers, "~> 1.3"},
      {:plug, "~> 1.12"},
      {:uniq, "~> 0.6"}
    ]
  end

  defp aliases do
    [
      reset: [],
      setup: "deps.get"
    ]
  end
end

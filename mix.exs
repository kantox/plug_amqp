defmodule PlugAmqp.MixProject do
  use Mix.Project

  @source_url "https://github.com/kantox/plug_amqp"
  @version "2.1.0"
  @description "A Plug adapter for Cowboy"

  def project do
    [
      app: :plug_amqp,
      version: @version,
      elixir: "~> 1.12",
      deps: deps(),
      test_coverage: [tool: ExCoveralls],
      package: package(),
      description: @description,
      name: "Plug.AMQP",
      docs: docs()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:amqp, "~> 3.0"},
      {:amqp_helpers, "~> 1.3"},
      {:dialyxir, "~> 1.4", only: [:dev, :test]},
      {:excoveralls, "~> 0.18.5", only: :test},
      {:ex_doc, "~> 0.37", only: :dev, runtime: false},
      {:plug, "~> 1.12"},
      {:uniq, "~> 0.6"}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "Plug.AMQP",
      source_ref: "v#{@version}",
      source_url: @source_url
    ]
  end
end

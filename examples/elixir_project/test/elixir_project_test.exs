defmodule ElixirProjectTest do
  use ExUnit.Case
  require Logger
  doctest ElixirProject

  test "greets the world" do
    assert ElixirProject.hello() == :world
  end

  test "load rocker" do
    {:ok, db} = :rocker.open_default("/tmp/db_test")
    assert is_reference(db) == true
  end
end

defmodule Bedrock.ControlPlane.Director.PeerRecoveryTest do
  use ExUnit.Case, async: false

  alias Bedrock.Test.Recovery.Peers

  @moduletag :distributed
  @moduletag :tmp_dir
  @moduletag timeout: 120_000

  test "a committed checkpoint survives loss of a log and system-cache node", %{tmp_dir: directory} do
    checkpoint_recovers!(directory, :system_cache)
  end

  test "a new leader restores a checkpoint after followers booted without a bootstrap", %{tmp_dir: directory} do
    checkpoint_recovers!(directory, :leader)
  end

  defp checkpoint_recovers!(directory, failure) do
    {_, 0} = System.cmd("epmd", ["-daemon"])
    cookie = Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
    peers = for _ <- 1..4, do: Peers.start!(cookie, &on_exit/1)
    nodes = Enum.map(peers, &elem(&1, 1))

    for {{peer, node}, index} <- Enum.with_index(peers) do
      for other <- nodes, other != node, do: assert(true == :peer.call(peer, Node, :connect, [other]))

      assert :ok =
               Peers.call(peer, :prepare, [
                 Path.join(directory, "node-#{index}"),
                 Path.join(directory, "objects"),
                 nodes
               ])
    end

    for {peer, _} <- peers, do: assert({:ok, _} = Peers.call(peer, :boot))
    {control, _} = hd(peers)
    {count, layout} = Peers.call(control, :ready)
    assert map_size(layout.logs) == 3
    location = Peers.call(control, :location)
    assert length(Enum.uniq(location.log_nodes)) == 3

    lost_node =
      case failure do
        :system_cache -> Enum.find(location.log_nodes, &(Atom.to_string(&1) in location.system_nodes))
        :leader -> location.leader
      end

    assert lost_node
    {lost, ^lost_node} = Enum.find(peers, &(elem(&1, 1) == lost_node))
    {survivor, _} = Enum.find(peers, &(elem(&1, 1) != lost_node))
    first = :erlang.term_to_binary(%{id: "agent-1", revision: 1, state: %{value: 31}})
    second = :erlang.term_to_binary(%{id: "agent-1", revision: 2, state: %{value: 32}})
    assert {:ok, :ok} = Peers.call(lost, :checkpoint, [nil, first])
    assert {:ok, ^first} = Peers.call(survivor, :read)

    os_pid = :peer.call(lost, System, :pid, [])
    monitor = Process.monitor(lost)
    assert {_, 0} = System.cmd("kill", ["-9", os_pid])
    assert_receive {:DOWN, ^monitor, :process, ^lost, _}, 10_000

    {recovered_count, recovered} = Peers.call(survivor, :ready, [count])
    assert recovered_count > count
    assert map_size(recovered.logs) == 3
    assert Peers.call(survivor, :location).cluster_id == location.cluster_id
    assert {:ok, ^first} = Peers.call(survivor, :read)
    assert {:ok, :ok} = Peers.call(survivor, :checkpoint, [first, second])
    assert {:error, :conflict} = Peers.call(survivor, :checkpoint, [first, first])
    assert {:ok, ^second} = Peers.call(survivor, :read)
  end
end

defmodule Bedrock.ControlPlane.Coordinator.InitializationTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :tmp_dir

  defmodule TestCluster do
    @moduledoc false

    def fetch_coordinator_nodes, do: {:ok, [Node.self()]}
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
    def otp_name(component), do: :"coordinator_initialization_test_#{component}"
  end

  setup %{tmp_dir: tmp_dir} do
    backend = ObjectStorage.backend(LocalFilesystem, root: tmp_dir)
    Application.put_env(:bedrock, TestCluster, object_storage: backend)
    on_exit(fn -> Application.delete_env(:bedrock, TestCluster) end)
    %{backend: backend}
  end

  test "bootstrap logs are recovery input, not a runnable layout", %{backend: backend} do
    bootstrap = %{
      cluster_id: "cluster-1",
      epoch: 7,
      logs: [%{id: "old-log", otp_ref: nil, shard_tags: []}],
      coordinators: [%{node: Atom.to_string(Node.self())}]
    }

    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))

    assert {:ok, state, {:continue, :check_recovery_consensus}} =
             Server.init({TestCluster, TestCluster.otp_name(:coordinator)})

    assert state.prior_core_state == %{logs: %{"old-log" => []}, system_materializers: %{}}
    assert state.transaction_system_layout == nil
  end

  test "a new leader refreshes recovery input from object storage", %{backend: backend} do
    bootstrap = %{
      cluster_id: "cluster-1",
      epoch: 7,
      logs: [%{id: "current-log", otp_ref: nil, shard_tags: []}],
      system_materializers: [%{id: "current-system", node: Atom.to_string(Node.self())}],
      coordinators: [%{node: Atom.to_string(Node.self())}]
    }

    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))

    state = %State{
      cluster: TestCluster,
      epoch: 8,
      config: %{stale: true},
      prior_core_state: %{logs: %{"stale-log" => []}, system_materializers: %{}}
    }

    refreshed = Server.refresh_recovery_state(state)

    assert refreshed.epoch == 8

    assert refreshed.prior_core_state == %{
             logs: %{"current-log" => []},
             system_materializers: %{"current-system" => Atom.to_string(Node.self())}
           }

    refute Map.has_key?(refreshed.config, :stale)
  end
end

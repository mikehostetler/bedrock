defmodule Bedrock.ControlPlane.Coordinator.PathConfigTest do
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft
  alias Bedrock.Raft.Log.TupleInMemoryLog
  alias Bedrock.Raft.Mode.Follower

  @moduletag :tmp_dir

  # Mock cluster to test the path configuration pattern
  defmodule TestPathCluster do
    @moduledoc false
    def node_config do
      Application.get_env(:bedrock, __MODULE__, [])
    end
  end

  # Helper to configure cluster with coordinator path
  defp configure_cluster_with_path(coordinator_path) do
    Application.put_env(:bedrock, TestPathCluster, coordinator: [path: coordinator_path])
  end

  # Helper to configure cluster without path
  defp configure_cluster_without_path do
    Application.put_env(:bedrock, TestPathCluster, coordinator: [])
  end

  describe "coordinator path configuration pattern" do
    test "coordinator creates disk log and files when path provided", %{tmp_dir: tmp_dir} do
      coordinator_path = Path.join(tmp_dir, "coordinator")
      configure_cluster_with_path(coordinator_path)

      # Verify it returns a DiskRaftLog and creates expected directory structure
      assert {:ok, %DiskRaftLog{} = raft_log} = Server.init_raft_log(TestPathCluster)

      working_directory = Path.join(coordinator_path, "raft")
      assert File.exists?(working_directory)
      assert File.exists?(Path.join(working_directory, "raft_log.dets"))

      DiskRaftLog.close(raft_log)
    end

    test "coordinator uses in-memory log when no path provided" do
      configure_cluster_without_path()

      assert {:ok, %TupleInMemoryLog{}} = Server.init_raft_log(TestPathCluster)
    end

    test "coordinator closes its disk log when it stops", %{tmp_dir: tmp_dir} do
      configure_cluster_with_path(Path.join(tmp_dir, "coordinator"))
      assert {:ok, %DiskRaftLog{} = raft_log} = Server.init_raft_log(TestPathCluster)

      raft = %Raft{mode: %Follower{log: raft_log}}

      assert :ok = Server.terminate(:normal, %State{raft: raft})
      assert :undefined == :dets.info(raft_log.table_name)
    end
  end
end

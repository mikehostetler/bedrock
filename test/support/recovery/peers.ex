defmodule Bedrock.Test.Recovery.Cluster do
  @moduledoc false
  use Bedrock.Cluster, otp_app: :bedrock, name: "bedrock_checkpoint_recovery"
end

defmodule Bedrock.Test.Recovery.Repo do
  @moduledoc false
  use Bedrock.Repo, cluster: Bedrock.Test.Recovery.Cluster
end

defmodule Bedrock.Test.Recovery.Peers do
  @moduledoc false
  use GenServer

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap
  alias Bedrock.Test.Recovery.Cluster
  alias Bedrock.Test.Recovery.Repo

  def start!(cookie, on_exit) do
    {:ok, peer, node} =
      :peer.start(%{
        name: :peer.random_name(~c"bedrock_recovery"),
        host: ~c"127.0.0.1",
        longnames: true,
        connection: :standard_io,
        args: [~c"+S", ~c"2", ~c"-setcookie", String.to_charlist(cookie)],
        wait_boot: 15_000
      })

    on_exit.(fn -> if Process.alive?(peer), do: :peer.stop(peer) end)
    :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()])
    {:ok, _} = :peer.call(peer, Application, :ensure_all_started, [:bedrock], 20_000)
    # Mix adds test dependencies to the application spec. The real peer
    # workload needs no mocks; avoid competing global Mox ownership servers.
    :peer.call(peer, Application, :stop, [:mox])
    {peer, node}
  end

  def call(peer, function, args \\ []), do: :peer.call(peer, __MODULE__, function, args, 45_000)

  def prepare(directory, objects, nodes) do
    File.mkdir_p!(directory)
    descriptor = Path.join(directory, "bedrock.cluster")
    :ok = Descriptor.write_to_file!(descriptor, Descriptor.new(Cluster.name(), nodes))
    backend = ObjectStorage.backend(LocalFilesystem, root: objects)

    Application.put_env(:bedrock, Cluster,
      capabilities: [:coordination, :log, :materializer],
      path_to_descriptor: descriptor,
      object_storage: backend,
      coordinator: [path: Path.join(directory, "coordinator"), persistent: true],
      worker: [path: Path.join(directory, "workers"), object_storage: backend],
      durability_mode: :strict,
      durability: [desired_logs: 3, desired_replication_factor: 3]
    )

    # Current Olivine reads snapshots from app config. Each peer has only
    # this cluster, and both config locations name the same shared store.
    Application.put_env(:bedrock, ObjectStorage, backend: backend)
    {:ok, _} = GenServer.start(__MODULE__, [], name: __MODULE__)
    :telemetry.attach(__MODULE__, [:bedrock, :recovery, :layout_persisted], &__MODULE__.layout/4, nodes)
  end

  def boot, do: GenServer.call(__MODULE__, :boot, 30_000)

  def ready(after_count \\ 0) do
    {count, director} = GenServer.call(__MODULE__, {:after_layout, after_count}, 30_000)
    %{distributor: distributor} = :sys.get_state(director, 10_000)
    :sys.get_state(distributor, 10_000)
    {:ok, _} = Repo.transact(fn -> {:ok, Repo.get("checkpoint/readiness")} end, timeout_in_ms: 10_000)
    :ok = Bedrock.Durability.require(Cluster, :strict)
    {count, Cluster.transaction_system_layout!()}
  end

  def location do
    backend = Keyword.fetch!(Cluster.node_config(), :object_storage)
    {:ok, binary} = ObjectStorage.get(backend, "bootstrap")
    {:ok, bootstrap} = ClusterBootstrap.read(binary)
    {:ok, directory} = Coordinator.fetch_service_directory(Cluster.otp_name(:coordinator))
    coordinator = :sys.get_state(Cluster.otp_name(:coordinator), 5_000)

    %{
      cluster_id: bootstrap.cluster_id,
      leader: coordinator.leader_node,
      system_nodes: Enum.map(bootstrap.system_materializers, & &1.node),
      log_nodes: for({_, {:log, {_, node}}} <- directory, do: node)
    }
  end

  def checkpoint(expected, next) do
    Repo.transact(
      fn ->
        if Repo.get("checkpoint/agent") == expected do
          :ok = Repo.put("checkpoint/agent", next)
          {:ok, :ok}
        else
          {:error, :conflict}
        end
      end,
      retry_limit: 0,
      timeout_in_ms: 10_000
    )
  end

  def read, do: Repo.transact(fn -> {:ok, Repo.get("checkpoint/agent")} end, timeout_in_ms: 10_000)

  def layout(_event, _measurements, _metadata, nodes) do
    for node <- nodes, do: GenServer.cast({__MODULE__, node}, {:layout, self()})
  end

  @impl true
  def init([]), do: {:ok, %{count: 0, director: nil, waiters: []}}

  @impl true
  def handle_call(:boot, _from, state) do
    result = Supervisor.start_link([Cluster], strategy: :one_for_one)
    {:reply, result, state}
  end

  def handle_call({:after_layout, count}, from, state) do
    if state.count > count,
      do: {:reply, {state.count, state.director}, state},
      else: {:noreply, %{state | waiters: [{from, count} | state.waiters]}}
  end

  @impl true
  def handle_cast({:layout, director}, state) do
    count = state.count + 1
    {done, waiting} = Enum.split_with(state.waiters, &(elem(&1, 1) < count))
    for {caller, _} <- done, do: GenServer.reply(caller, {count, director})
    {:noreply, %{state | count: count, director: director, waiters: waiting}}
  end
end

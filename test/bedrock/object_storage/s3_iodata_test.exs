defmodule Bedrock.ObjectStorage.S3IodataTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.S3
  alias Bedrock.ObjectStorage.Snapshot

  defmodule HTTPClient do
    @moduledoc false
    @behaviour ExAws.Request.HttpClient

    @impl true
    def request(method, _url, body, headers, _opts) do
      send(self(), {:http_request, method, body, headers})
      {:ok, %{status_code: 200, headers: [{"etag", "\"current\""}], body: ""}}
    end
  end

  setup do
    backend =
      ObjectStorage.backend(S3,
        bucket: "iodata-test",
        config: [
          access_key_id: "test-key",
          secret_access_key: "test-secret",
          region: "us-east-1",
          http_client: HTTPClient,
          retries: [max_attempts: 1]
        ]
      )

    %{backend: backend}
  end

  test "put sends nested iodata as exact binary bytes", %{backend: backend} do
    data = [<<255, 0>>, ["data", 1], "index"]
    assert :ok = ObjectStorage.put(backend, "objects/data", data)
    assert_put(IO.iodata_to_binary(data))
  end

  test "conditional create sends nested iodata and retains the condition", %{backend: backend} do
    data = [<<255>>, ["data", 0], "index"]
    assert :ok = ObjectStorage.put_if_not_exists(backend, "objects/data", data)
    headers = assert_put(IO.iodata_to_binary(data))
    assert {"if-none-match", "*"} in headers
  end

  test "conditional update sends nested iodata and retains the token", %{backend: backend} do
    data = ["data", [<<255>>, 0], "index"]
    assert :ok = ObjectStorage.put_if_version_matches(backend, "objects/data", "\"current\"", data)
    assert_received {:http_request, :head, "", _headers}
    headers = assert_put(IO.iodata_to_binary(data))
    assert {"if-match", "\"current\""} in headers
  end

  test "snapshot write sends the data and index as one binary bundle", %{backend: backend} do
    snapshot = Snapshot.new(backend, "0")
    assert :ok = Snapshot.write(snapshot, 17, [<<255, 0>>, "index"])
    assert_put(<<255, 0, "index">>)
  end

  test "binary writes retain their exact bytes", %{backend: backend} do
    assert :ok = ObjectStorage.put(backend, "objects/binary", <<255, 0, 1>>)
    assert_put(<<255, 0, 1>>)
  end

  defp assert_put(expected) do
    assert_received {:http_request, :put, ^expected, headers}
    assert {"content-length", Integer.to_string(byte_size(expected))} in headers
    hash = :sha256 |> :crypto.hash(expected) |> Base.encode16(case: :lower)
    assert {"x-amz-content-sha256", hash} in headers
    headers
  end
end

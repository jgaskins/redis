require "./spec_helper"
require "uuid"

require "../src/graph"

private macro test(name, &block)
  it {{name}} do
    key = UUID.random.to_s
    graph = redis.graph(key)

    begin
      {{yield}}
    ensure
      redis.del key
    end
  end
end

describe Redis::Graph do
  redis = Redis::Client.new

  test "creates and retrieves nodes" do
    2.times do
      result = graph.write_query("CREATE (user:User { id: randomUUID() }) RETURN user")
      Redis::Graph::Result.new(result).size.should eq 1
    end

    result = Redis::Graph::Result.new(graph.read_query("MATCH (user:User) RETURN user"))

    result.size.should eq 2
  end

  test "runs queries with params" do
    result = graph.write_query <<-CYPHER, id: 123
      CREATE (user:User { id: $id })
      RETURN user
    CYPHER

    count = 0
    Redis::Graph::Result.new(result).each do |(user)|
      count += 1
    end
    count.should eq 1

    # pp value: result.first, type: typeof(result.first)
    # result.first.first.properties["id"].should eq 123
  end
end

describe Redis::Graph::Result do
  it "translates the raw result into a usable value" do
    raw = [
      ["user"],
      [
        [
          [
            ["id", 0i64],
            ["labels", ["User"]],
            ["properties", [["id", "ecb2f33b-beaf-4ce0-b261-fce4c02daff3"]]],
          ],
        ],
      ],
      [
        "Labels added: 1",
        "Nodes created: 2",
        "Properties set: 3",
        "Cached execution: 1",
        "Query internal execution time: 0.196421 milliseconds",
      ],
    ]

    result = Redis::Graph::Result.new(raw)

    result.size.should eq 1
    result.columns.should eq %w[user]
    result.labels_added.should eq 1
    result.nodes_created.should eq 2
    result.properties_set.should eq 3
    result.duration.should eq 0.196421.milliseconds
    result.cached_execution?.should eq false

    user = result.first.first.as(Redis::Graph::Node)
    user.labels.should eq %w[User]
    user.id.should eq 0
    user.properties.should eq({"id" => "ecb2f33b-beaf-4ce0-b261-fce4c02daff3"})
  end
end

require "./spec_helper"
require "uuid"

require "../src/graph"

struct Person
  include Redis::Graph::Serializable::Node

  getter id : UUID
  getter name : String
  getter value : Int32?
  getter created_at : Time
end

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

pending Redis::Graph do
  redis = Redis::Client.new

  test "creates and retrieves nodes" do
    2.times do
      result = graph.write_query("CREATE (user:User { id: randomUUID() }) RETURN user")
      result.size.should eq 1
    end

    result = graph.read_query("MATCH (user:User) RETURN user")

    result.size.should eq 2
  end

  test "runs queries with params" do
    result = graph.write_query <<-CYPHER, id: 123
      CREATE (user:User { id: $id })
      RETURN user
    CYPHER

    count = 0
    result.each do |(user)|
      count += 1
    end
    count.should eq 1

    result.first.first.as(Redis::Graph::Node).properties["id"].should eq 123
  end

  test "runs queries returning scalars" do
    result = graph.read_query <<-CYPHER, return: {Int32, String}
      RETURN
        42 AS answer,
        'hello' AS greeting
    CYPHER

    result.first.should eq({42, "hello"})
  end

  test "runs queries on custom types" do
    id = UUID.random
    result = graph.write_query <<-CYPHER, {id: id, name: "Jamie", created_at: Time.utc.to_unix_ms}, return: {Person}
      CREATE (user:User {
        id: $id,
        name: $name,
        created_at: $created_at
      })
      RETURN user
    CYPHER

    count = 0
    jamie = uninitialized Person
    result.each do |(person)|
      count += 1
      jamie = person
      person.name.should eq "Jamie"
      person.created_at.should be_within(1.second, of: Time.utc)
    end
    count.should eq 1

    result = graph.read_query <<-CYPHER, {id: jamie.id}, return: {Person?}
      MATCH (user:User {id: $id})
      RETURN user
      LIMIT 1
    CYPHER

    count = 0
    result.each do |(person)|
      count += 1
      person.should eq jamie
    end
    count.should eq 1
  end

  test "runs queries on a single custom type" do
    result = graph.read_query "RETURN $value", {value: "hello"}, return: String
    result.size.should eq 1
    result.each do |string|
      string.should eq "hello"
    end

    id = UUID.random
    result = graph.write_query <<-CYPHER, {id: id, name: "Jamie", created_at: Time.utc.to_unix_ms}, return: Person
      CREATE (user:User {
        id: $id,
        name: $name,
        created_at: $created_at
      })
      RETURN user
    CYPHER

    count = 0
    jamie = uninitialized Person
    result.each do |person|
      count += 1
      jamie = person
      person.name.should eq "Jamie"
      person.created_at.should be_within(1.second, of: Time.utc)
    end
    count.should eq 1

    result = graph.read_query <<-CYPHER, {id: jamie.id}, return: Person
      MATCH (user:User {id: $id})
      RETURN user
      LIMIT 1
    CYPHER

    count = 0
    result.each do |person|
      count += 1
      person.should eq jamie
    end
    count.should eq 1
  end

  test "can use transactions" do
    id = UUID.random
    begin
      graph.multi do |txn|
        txn.write_query <<-CYPHER, id: id.to_s
          CREATE (p:Person{id: $id})
        CYPHER

        result = txn.read_query <<-CYPHER, {id: id}, return: {Person}
          MATCH (p:Person{id: $id})
          RETURN p
        CYPHER

        result.size.should eq 1

        raise "hell" # Abort the txn
      end
    rescue # Don't fail the spec because we raised on purpose
    end

    result = graph.read_query <<-CYPHER, {id: id}, return: {Person}
      MATCH (p:Person{id: $id})
      RETURN p
    CYPHER

    result.size.should eq 0
  end
end

require "../src/writer"
describe Redis::Graph::Result do
  it "translates the raw result into a usable value" do
    # Specifying types all the way down is complicated, so we're just letting
    # the normal Redis I/O handle it. This better simulates what happens anyway.
    buffer = IO::Memory.new
    writer = Redis::Writer.new(buffer)
    writer.encode [
      ["user", "membership"],
      [
        [
          [
            ["id", 0i64],
            ["labels", ["User"]],
            ["properties", [["id", "ecb2f33b-beaf-4ce0-b261-fce4c02daff3"]]],
          ],
          [
            ["id", 321i64],
            ["type", "MEMBER_OF"],
            ["src_node", 0i64],
            ["dest_node", 1i64],
            ["properties", [["since", 1234567890i64]]],
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
    raw = Redis::Parser.new(buffer.rewind).read.as(Array)

    result = Redis::Graph::Result.new(raw)

    result.size.should eq 1
    result.fields.should eq %w[user membership]
    result.labels_added.should eq 1
    result.nodes_created.should eq 2
    result.properties_set.should eq 3
    result.duration.should eq 0.196421.milliseconds
    result.cached_execution?.should eq true

    user = result.first.first.as(Redis::Graph::Node)
    user.labels.should eq %w[User]
    user.id.should eq 0
    user.properties.should eq({"id" => "ecb2f33b-beaf-4ce0-b261-fce4c02daff3"})

    membership = result.first[1].as(Redis::Graph::Relationship)
    membership.type.should eq "MEMBER_OF"
    membership.id.should eq 321
    membership.src_node.should eq 0
    membership.dest_node.should eq 1
    membership.properties.should eq({"since" => 1234567890})
  end
end

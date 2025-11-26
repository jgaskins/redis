require "./spec_helper"
require "uuid"

require "../src/graph"

struct Person
  include Redis::Graph::Serializable::Node

  getter id : UUID
  getter name : String
  getter optional_value : Int32?
  getter coordinates : Redis::Graph::Point?
  getter created_at : Time
end

struct Group
  include Redis::Graph::Serializable::Node

  getter id : UUID
  getter name : String
  getter created_at : Time
end

struct GroupMembership
  include Redis::Graph::Serializable::Relationship

  getter role : Role
  getter created_at : Time

  enum Role
    Member
    Admin
    Owner
  end
end

private macro test(name, **kwargs, &block)
  it {{name}}{% for key, value in kwargs %}, {{key}}: {{value}}{% end %} do
    key = UUID.random.to_s
    graph = redis.graph(key)

    begin
      # Ensure the graph exists before we start trying to do things with it
      graph.write_query "RETURN 42" rescue nil

      {{yield}}
    ensure
      graph.delete!
    end
  end
end

if url = ENV["REDIS_GRAPH_URL"]?
  describe Redis::Graph do
    redis = Redis::Client.new(URI.parse(url))

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
      # Need to ensure the graph key exists before running a read query on it
      graph.write_query <<-CYPHER, NamedTuple.new, return: String
      RETURN "42"
      CYPHER

      result = graph.read_query <<-CYPHER, return: {Int32, String}
      RETURN
        42 AS answer,
        'hello' AS greeting
    CYPHER

      result.first.should eq({42, "hello"})
    end

    test "runs queries on custom types" do
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

      result = graph.read_query <<-CYPHER, {id: jamie.id}, return: Person?
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

    test "runs queries on a multiple custom types" do
      result = graph.read_query "RETURN $one, $two", {one: 1, two: 2}, return: {Int32, Int64}
      result.size.should eq 1
      result.first.should eq({1, 2})

      person_id = UUID.v4
      group_id = UUID.v4
      created_at = Time.utc
      params = {
        person_id:         person_id,
        person_name:       "Jamie",
        person_created_at: created_at.to_unix_ms,
        group_id:          group_id,
        group_name:        "My Group",
        membership_role:   GroupMembership::Role::Owner,
      }
      result = graph.write_query <<-CYPHER, params, return: {Person, GroupMembership, Group}
      CREATE (user:Person {
        id: $person_id,
        name: $person_name,
        created_at: $person_created_at
      })
      CREATE (group:Group {
        id: $group_id,
        name: $group_name,
        created_at: timestamp()
      })
      CREATE (user)-[membership:MEMBER_OF{role: $membership_role, created_at: timestamp()}]->(group)
      RETURN user, membership, group
    CYPHER

      result.size.should eq 1
      result.first.should be_a({Person, GroupMembership, Group})
      person, membership, group = result.first
      person.id.should eq person_id
      person.name.should eq "Jamie"
      person.created_at.should be_within 1.millisecond, of: created_at
      group.id.should eq group_id
      group.name.should eq "My Group"
      membership.role.owner?.should eq true
      membership.created_at.should be_within 5.milliseconds, of: Time.utc
    end

    test "can deserialize Points" do
      results = graph.write_query <<-CYPHER
      RETURN point({latitude: 1.0, longitude: 2.0}) AS p
      CYPHER

      results.first.first.should eq Redis::Graph::Point.new(1.0, 2.0)

      results = graph.write_query(<<-CYPHER, {name: "Jamie", coordinates: Redis::Graph::Point.new(3.0, 4.0)}, return: Person)
      CREATE (p:Person{
        id: randomUUID(),
        name: $name,
        coordinates: $coordinates,
        created_at: timestamp()
      })
      RETURN p
      CYPHER

      results.first.coordinates.should eq Redis::Graph::Point.new(3.0, 4.0)
    end

    test "can explain a query" do
      graph.write_query "CREATE INDEX FOR (user:User) ON (user.email)"
      graph.write_query "CREATE INDEX FOR (c:Credential) ON (c.value)"
      graph.write_query "CREATE INDEX FOR (r:Release) ON (r.version)"

      graph.write_query <<-CYPHER
      CREATE (jamie:User{id: "jgaskins", name: "Jamie", email: "jgaskins@example.com"})

      CREATE (db:Facet{name: "db"})
      CREATE (interro:Facet{name: "interro"})

      CREATE (interro)-[:HAS_RELEASE]->(interro_0_2_5:Release{version: "0.2.5", version_split: [0, 2, 5]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_2_4:Release{version: "0.2.4", version_split: [0, 2, 4]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_2_3:Release{version: "0.2.3", version_split: [0, 2, 3]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_2_2:Release{version: "0.2.2", version_split: [0, 2, 2]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_2_1:Release{version: "0.2.1", version_split: [0, 2, 1]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_2_0:Release{version: "0.2.0", version_split: [0, 2, 0]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_1_8:Release{version: "0.1.8", version_split: [0, 1, 8]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_1_7:Release{version: "0.1.7", version_split: [0, 1, 7]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_1_6:Release{version: "0.1.6", version_split: [0, 1, 6]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_1_5:Release{version: "0.1.5", version_split: [0, 1, 5]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_1_4:Release{version: "0.1.4", version_split: [0, 1, 4]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_1_3:Release{version: "0.1.3", version_split: [0, 1, 3]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_1_2:Release{version: "0.1.2", version_split: [0, 1, 2]})
      CREATE (interro)-[:HAS_RELEASE]->(interro_0_1_1:Release{version: "0.1.1", version_split: [0, 1, 1]})
      CYPHER

      result = graph.explain <<-CYPHER, {email: "me@example.com", credential_type: 0}
      MATCH (user:User{email: $email})-[:HAS_CREDENTIAL]->(c:Credential{value: $credential})

      RETURN user
      LIMIT 1
      CYPHER

      result.should eq [
        "Results",
        "    Limit",
        "        Project",
        "            Filter",
        "                Conditional Traverse | (user)->(c:Credential)",
        "                    Node By Index Scan | (user:User)",
      ]
    end

    test "can use transactions" do
      id = UUID.random
      begin
        graph.multi do |txn|
          txn.write_query <<-CYPHER, id: id.to_s
          CREATE (p:Person{id: $id})
        CYPHER

          result = txn.read_query <<-CYPHER, {id: id}, return: Person
          MATCH (p:Person{id: $id})
          RETURN p
        CYPHER

          result.size.should eq 1

          raise "hell" # Abort the txn
        end
      rescue # Don't fail the spec because we raised on purpose
      end

      result = graph.read_query <<-CYPHER, {id: id}, return: Person
      MATCH (p:Person{id: $id})
      RETURN p
    CYPHER

      # Failing the transaction rolls back the `CREATE` query we did above
      result.size.should eq 0
    end

    describe "constraints" do
      test "creates unique node constraints" do
        # TODO: Indices and constraints are asynchronous, so this might fail if
        # we manage to send the queries faster than it creates the index and
        # constraint.
        graph.indices.create "Person", "id"
        graph.constraints.create "Person", :unique, :node, property: "id"
        id = UUID.random

        graph.write_query <<-CYPHER, id: id
        CREATE (p:Person{id: $id})
        CYPHER

        expect_raises Redis::Graph::ConstraintViolation do
          graph.write_query <<-CYPHER, id: id
          CREATE (p:Person{id: $id})
          CYPHER
        end
      end

      test "lists constraints" do
        graph.indices.create "Person", "id"
        graph.constraints.create "Person", :unique, :node, property: "id"
        graph.constraints.create "MEMBER_OF", :mandatory, :relationship, property: "since"

        # Listing node constraints
        graph.constraints.list(node: "Person").first.properties.should eq %w[id]

        # Listing all constraints
        constraints = graph.constraints.list
        person_constraint = constraints.find! { |c| c.label == "Person" }
        person_constraint.type.unique?.should eq true
        person_constraint.entity_type.node?.should eq true
        membership_constraint = constraints.find! { |c| c.label == "MEMBER_OF" }
        membership_constraint.type.mandatory?.should eq true
        membership_constraint.entity_type.relationship?.should eq true
      end
    end
  end
end

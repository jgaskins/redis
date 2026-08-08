require "./spec_helper"
require "../src/replication_client"

module Redis
  describe ReplicationClient do
    describe ".parse_replication_section" do
      it "parses the master's replication section" do
        section = <<-SECTION
          # Replication\r
          role:master\r
          connected_slaves:2\r
          slave0:ip=10.76.3.39,port=6379,state=stable_sync,lag=0\r
          slave1:ip=10.76.1.130,port=6379,state=stable_sync,lag=0\r
          master_replid:b08ca5082296cf5b2c1de7207f2bc16bb8da3d80\r

          SECTION

        data = ReplicationClient::Info::Replication.new(section)

        data.role.master?.should eq true
        data.connected_replicas.should eq 2
        data.replicas.should contain ReplicationClient::Info::Replica.new(
          ip: "10.76.3.39",
          port: 6379,
          state: :stable_sync,
          lag: 0.seconds,
        )
      end

      it "ignores non-replica slave_* fields and unknown replica entry keys" do
        section = <<-SECTION
          # Replication\r
          role:master\r
          connected_slaves:2\r
          slave0:ip=cache-1.cache.redis-operator-example.svc.cluster.local,port=6379,state=online,offset=23804150,lag=0,io-thread=0\r
          slave1:ip=cache-0.cache.redis-operator-example.svc.cluster.local,port=6379,state=online,offset=23804498,lag=0,io-thread=0\r
          master_failover_state:no-failover\r
          master_replid:195f2aaf474454e689dd52c1968748d407409798\r
          master_replid2:3408492d325c40f4659a8823df3c7b6bc20ff1f5\r
          master_repl_offset:23804498\r
          second_repl_offset:18990135\r
          repl_backlog_active:1\r
          repl_backlog_size:1048576\r
          repl_backlog_first_byte_offset:22743268\r
          repl_backlog_histlen:1061231\r

          SECTION

        data = ReplicationClient::Info::Replication.new(section)

        data.role.master?.should eq true
        data.connected_replicas.should eq 2
        data.replicas.size.should eq 2
        data.replicas[0].ip.should eq "cache-1.cache.redis-operator-example.svc.cluster.local"
        data.replicas[0].port.should eq 6379
        data.replicas[0].state.online?.should eq true
        data.replicas[0].lag.should eq 0.seconds
      end

      it "parses a replica's replication section" do
        section = <<-SECTION
          # Replication\r
          role:replica\r
          master_host:10.76.2.33\r
          master_port:9999\r
          master_link_status:up\r
          master_last_io_seconds_ago:0\r
          master_sync_in_progress:0\r

          SECTION

        data = ReplicationClient::Info::Replication.new(section)

        data.role.master?.should eq false
        data.role.replica?.should eq true
        data.master_host.should eq "10.76.2.33"
        data.master_port.should eq 9999
        data.master_link_status.should eq "up"
        data.master_last_io.not_nil!.should be_within 1.seconds, of: Time.utc
        data.master_sync_in_progress?.should eq false
      end
    end
  end
end

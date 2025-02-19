package gocql

import (
	"log"
	"time"

	"github.com/kulezi/scylla-go-driver"
	"github.com/kulezi/scylla-go-driver/transport"
)

type ClusterConfig struct {
	// addresses for the initial connections. It is recommended to use the value set in
	// the Cassandra config for broadcast_address or listen_address, an IP address not
	// a domain name. This is because events from Cassandra will use the configured IP
	// address, which is used to index connected hosts. If the domain name specified
	// resolves to more than 1 IP address then the driver may connect multiple times to
	// the same host, and will not mark the node being down or up from events.
	Hosts []string

	// CQL version (default: 3.0.0)
	CQLVersion string

	// ProtoVersion sets the version of the native protocol to use, this will
	// enable features in the driver for specific protocol versions, generally this
	// should be set to a known version (2,3,4) for the cluster being connected to.
	//
	// If it is 0 or unset (the default) then the driver will attempt to discover the
	// highest supported protocol for the cluster. In clusters with nodes of different
	// versions the protocol selected is not defined (ie, it can be any of the supported in the cluster)
	ProtoVersion int

	// Connection timeout (default: 600ms)
	Timeout time.Duration

	// Initial connection timeout, used during initial dial to server (default: 600ms)
	// ConnectTimeout is used to set up the default dialer and is ignored if Dialer or HostDialer is provided.
	ConnectTimeout time.Duration

	// Port used when dialing.
	// Default: 9042
	Port int

	// Initial keyspace. Optional.
	Keyspace string

	// Number of connections per host.
	// Default: 2
	NumConns int

	// Default consistency level.
	// Default: Quorum
	Consistency Consistency

	// Compression algorithm.
	// Default: nil
	Compressor Compressor

	// Default: nil
	Authenticator Authenticator

	// An Authenticator factory. Can be used to create alternative authenticators.
	// Default: nil
	// AuthProvider func(h *HostInfo) (Authenticator, error)

	// Default retry policy to use for queries.
	// Default: no retries.
	RetryPolicy RetryPolicy

	// ConvictionPolicy decides whether to mark host as down based on the error and host info.
	// Default: SimpleConvictionPolicy
	ConvictionPolicy ConvictionPolicy // TODO: use it?

	// Default reconnection policy to use for reconnecting before trying to mark host as down.
	// ReconnectionPolicy ReconnectionPolicy

	// The keepalive period to use, enabled if > 0 (default: 0)
	// SocketKeepalive is used to set up the default dialer and is ignored if Dialer or HostDialer is provided.
	SocketKeepalive time.Duration

	// Maximum cache size for prepared statements globally for gocql.
	// Default: 1000
	MaxPreparedStmts int

	// Maximum cache size for query info about statements for each session.
	// Default: 1000
	MaxRoutingKeyInfo int

	// Default page size to use for created sessions.
	// Default: 5000
	PageSize int

	// Consistency for the serial part of queries, values can be either SERIAL or LOCAL_SERIAL.
	// Default: unset
	// SerialConsistency SerialConsistency

	// SslOpts configures TLS use when HostDialer is not set.
	// SslOpts is ignored if HostDialer is set.
	SslOpts *SslOptions

	// Sends a client side timestamp for all requests which overrides the timestamp at which it arrives at the server.
	// Default: true, only enabled for protocol 3 and above.
	DefaultTimestamp bool

	// PoolConfig configures the underlying connection pool, allowing the
	// configuration of host selection and connection selection policies.
	PoolConfig PoolConfig

	// If not zero, gocql attempt to reconnect known DOWN nodes in every ReconnectInterval.
	ReconnectInterval time.Duration // FIXME: unused

	// The maximum amount of time to wait for schema agreement in a cluster after
	// receiving a schema change frame. (default: 60s)
	MaxWaitSchemaAgreement time.Duration

	// HostFilter will filter all incoming events for host, any which don't pass
	// the filter will be ignored. If set will take precedence over any options set
	// via Discovery
	// HostFilter HostFilter

	// AddressTranslator will translate addresses found on peer discovery and/or
	// node change events.
	// AddressTranslator AddressTranslator

	// If IgnorePeerAddr is true and the address in system.peers does not match
	// the supplied host by either initial hosts or discovered via events then the
	// host will be replaced with the supplied address.
	//
	// For example if an event comes in with host=10.0.0.1 but when looking up that
	// address in system.local or system.peers returns 127.0.0.1, the peer will be
	// set to 10.0.0.1 which is what will be used to connect to.
	IgnorePeerAddr bool

	// If DisableInitialHostLookup then the driver will not attempt to get host info
	// from the system.peers table, this will mean that the driver will connect to
	// hosts supplied and will not attempt to lookup the hosts information, this will
	// mean that data_centre, rack and token information will not be available and as
	// such host filtering and token aware query routing will not be available.
	DisableInitialHostLookup bool

	// Configure events the driver will register for
	Events struct {
		// disable registering for status events (node up/down)
		DisableNodeStatusEvents bool
		// disable registering for topology events (node added/removed/moved)
		DisableTopologyEvents bool
		// disable registering for schema events (keyspace/table/function removed/created/updated)
		DisableSchemaEvents bool
	}

	// DisableSkipMetadata will override the internal result metadata cache so that the driver does not
	// send skip_metadata for queries, this means that the result will always contain
	// the metadata to parse the rows and will not reuse the metadata from the prepared
	// statement.
	//
	// See https://issues.apache.org/jira/browse/CASSANDRA-10786
	DisableSkipMetadata bool

	// QueryObserver will set the provided query observer on all queries created from this session.
	// Use it to collect metrics / stats from queries by providing an implementation of QueryObserver.
	// QueryObserver QueryObserver

	// BatchObserver will set the provided batch observer on all queries created from this session.
	// Use it to collect metrics / stats from batch queries by providing an implementation of BatchObserver.
	// BatchObserver BatchObserver

	// ConnectObserver will set the provided connect observer on all queries
	// created from this session.
	// ConnectObserver ConnectObserver

	// FrameHeaderObserver will set the provided frame header observer on all frames' headers created from this session.
	// Use it to collect metrics / stats from frames by providing an implementation of FrameHeaderObserver.
	// FrameHeaderObserver FrameHeaderObserver

	// Default idempotence for queries
	DefaultIdempotence bool

	// The time to wait for frames before flushing the frames connection to Cassandra.
	// Can help reduce syscall overhead by making less calls to write. Set to 0 to
	// disable.
	//
	// (default: 200 microseconds)
	WriteCoalesceWaitTime time.Duration

	// Dialer will be used to establish all connections created for this Cluster.
	// If not provided, a default dialer configured with ConnectTimeout will be used.
	// Dialer is ignored if HostDialer is provided.
	// Dialer Dialer

	// HostDialer will be used to establish all connections for this Cluster.
	// Unlike Dialer, HostDialer is responsible for setting up the entire connection, including the TLS session.
	// To support shard-aware port, HostDialer should implement ShardDialer.
	// If not provided, Dialer will be used instead.
	// HostDialer HostDialer

	// DisableShardAwarePort will prevent the driver from connecting to Scylla's shard-aware port,
	// even if there are nodes in the cluster that support it.
	//
	// It is generally recommended to leave this option turned off because gocql can use
	// the shard-aware port to make the process of establishing more robust.
	// However, if you have a cluster with nodes which expose shard-aware port
	// but the port is unreachable due to network configuration issues, you can use
	// this option to work around the issue. Set it to true only if you neither can fix
	// your network nor disable shard-aware port on your nodes.
	DisableShardAwarePort bool

	// Logger for this ClusterConfig.
	// If not specified, defaults to the global gocql.Logger.
	Logger StdLogger

	// internal config for testing
	disableControlConn bool
	disableInit        bool
}

func NewCluster(hosts ...string) *ClusterConfig {
	cfg := ClusterConfig{Hosts: hosts, WriteCoalesceWaitTime: 200 * time.Microsecond}
	return &cfg
}

func sessionConfigFromGocql(cfg *ClusterConfig) (scylla.SessionConfig, error) {
	scfg := scylla.DefaultSessionConfig(cfg.Keyspace, cfg.Hosts...)
	scfg.Hosts = cfg.Hosts
	scfg.WriteCoalesceWaitTime = cfg.WriteCoalesceWaitTime
	if _, ok := cfg.Compressor.(SnappyCompressor); ok {
		scfg.Compression = scylla.Snappy
	}

	if auth, ok := cfg.Authenticator.(PasswordAuthenticator); ok {
		scfg.Username = auth.Username
		scfg.Password = auth.Password
	}

	if policy, ok := cfg.PoolConfig.HostSelectionPolicy.(transport.HostSelectionPolicy); ok {
		scfg.HostSelectionPolicy = policy
	}

	if retryPolicy, ok := cfg.RetryPolicy.(transport.RetryPolicy); ok {
		scfg.RetryPolicy = retryPolicy
	}

	if cfg.Logger == nil {
		if Logger == nil {
			scfg.Logger = log.Default()
		} else {
			scfg.Logger = stdLoggerWrapper{Logger}
		}
	} else {
		if cfg.Logger == nil {
			cfg.Logger = log.Default()
		}
		scfg.Logger = stdLoggerWrapper{cfg.Logger}
	}

	if cfg.SslOpts != nil {
		tlsConfig, err := setupTLSConfig(cfg.SslOpts)
		if err != nil {
			return scylla.SessionConfig{}, err
		}
		scfg.TLSConfig = tlsConfig
	}

	return scfg, nil
}

func (cfg *ClusterConfig) CreateSession() (*Session, error) {
	return NewSession(*cfg)
}

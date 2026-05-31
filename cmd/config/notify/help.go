/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package notify

import (
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/event/target"
)

const (
	formatComment     = `'namespace' reflects current bucket/object list and 'access' reflects a journal of object operations, defaults to 'namespace'`
	queueDirComment   = `staging dir for undelivered messages e.g. '/home/events'`
	queueLimitComment = `maximum limit for undelivered messages, defaults to '100000'`
)

// Help template inputs for all notification targets
var (
	HelpWebhook = config.HelpKVS{
		config.HelpKV{
			Key:         target.WebhookEndpoint,
			Description: "webhook server endpoint e.g. http://localhost:8080/minio/events",
			Type:        "url",
		},
		config.HelpKV{
			Key:         target.WebhookAuthToken,
			Description: "opaque string or JWT authorization token",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.WebhookQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.WebhookQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         target.WebhookClientCert,
			Description: "client cert for Webhook mTLS auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.WebhookClientKey,
			Description: "client cert key for Webhook mTLS auth",
			Optional:    true,
			Type:        "string",
		},
	}

	HelpPostgres = config.HelpKVS{
		config.HelpKV{
			Key:         target.PostgresConnectionString,
			Description: `Postgres server connection-string e.g. "host=localhost port=5432 dbname=minio_events user=postgres password=password sslmode=disable"`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresTable,
			Description: "DB table name to store/update events, table is auto-created",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.PostgresFormat,
			Description: formatComment,
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.PostgresQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.PostgresQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         target.PostgresMaxOpenConnections,
			Description: "To set the maximum number of open connections to the database. The value is set to `2` by default.",
			Optional:    true,
			Type:        "number",
		},
	}

	HelpMySQL = config.HelpKVS{
		config.HelpKV{
			Key:         target.MySQLDSNString,
			Description: `MySQL data-source-name connection string e.g. "<user>:<password>@tcp(<host>:<port>)/<database>"`,
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLTable,
			Description: "DB table name to store/update events, table is auto-created",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.MySQLFormat,
			Description: formatComment,
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.MySQLQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.MySQLQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
		config.HelpKV{
			Key:         target.MySQLMaxOpenConnections,
			Description: "To set the maximum number of open connections to the database. The value is set to `2` by default.",
			Optional:    true,
			Type:        "number",
		},
	}

	HelpNATS = config.HelpKVS{
		config.HelpKV{
			Key:         target.NATSAddress,
			Description: "NATS server address e.g. '0.0.0.0:4222'",
			Type:        "address",
		},
		config.HelpKV{
			Key:         target.NATSSubject,
			Description: "NATS subscription subject",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSUsername,
			Description: "NATS username",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSPassword,
			Description: "NATS password",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSToken,
			Description: "NATS token",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSTLS,
			Description: "set to 'on' to enable TLS",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSTLSSkipVerify,
			Description: `trust server TLS without verification, defaults to "on" (verify)`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSPingInterval,
			Description: "client ping commands interval in s,m,h,d. Disabled by default",
			Optional:    true,
			Type:        "duration",
		},
		config.HelpKV{
			Key:         target.NATSStreaming,
			Description: "set to 'on', to use streaming NATS server",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSStreamingAsync,
			Description: "set to 'on', to enable asynchronous publish",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NATSStreamingMaxPubAcksInFlight,
			Description: "number of messages to publish without waiting for ACKs",
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.NATSStreamingClusterID,
			Description: "unique ID for NATS streaming cluster",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSCertAuthority,
			Description: "path to certificate chain of the target NATS server",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSClientCert,
			Description: "client cert for NATS mTLS auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSClientKey,
			Description: "client cert key for NATS mTLS auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NATSQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.NATSQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpNSQ = config.HelpKVS{
		config.HelpKV{
			Key:         target.NSQAddress,
			Description: "NSQ server address e.g. '127.0.0.1:4150'",
			Type:        "address",
		},
		config.HelpKV{
			Key:         target.NSQTopic,
			Description: "NSQ topic",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.NSQTLS,
			Description: "set to 'on' to enable TLS",
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NSQTLSSkipVerify,
			Description: `trust server TLS without verification, defaults to "on" (verify)`,
			Optional:    true,
			Type:        "on|off",
		},
		config.HelpKV{
			Key:         target.NSQQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.NSQQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpES = config.HelpKVS{
		config.HelpKV{
			Key:         target.ElasticURL,
			Description: "Elasticsearch server's address, with optional authentication info",
			Type:        "url",
		},
		config.HelpKV{
			Key:         target.ElasticIndex,
			Description: `Elasticsearch index to store/update events, index is auto-created`,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.ElasticFormat,
			Description: formatComment,
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.ElasticQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.ElasticQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         target.ElasticUsername,
			Description: "username for Elasticsearch basic-auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.ElasticPassword,
			Description: "password for Elasticsearch basic-auth",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}

	HelpRedis = config.HelpKVS{
		config.HelpKV{
			Key:         target.RedisAddress,
			Description: "Redis server's address. For example: `localhost:6379`",
			Type:        "address",
		},
		config.HelpKV{
			Key:         target.RedisKey,
			Description: "Redis key to store/update events, key is auto-created",
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.RedisFormat,
			Description: formatComment,
			Type:        "namespace*|access",
		},
		config.HelpKV{
			Key:         target.RedisPassword,
			Description: "Redis server password",
			Optional:    true,
			Type:        "string",
		},
		config.HelpKV{
			Key:         target.RedisQueueDir,
			Description: queueDirComment,
			Optional:    true,
			Type:        "path",
		},
		config.HelpKV{
			Key:         target.RedisQueueLimit,
			Description: queueLimitComment,
			Optional:    true,
			Type:        "number",
		},
		config.HelpKV{
			Key:         config.Comment,
			Description: config.DefaultComment,
			Optional:    true,
			Type:        "sentence",
		},
	}
)

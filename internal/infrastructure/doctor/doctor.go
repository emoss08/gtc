// Package doctor checks a deployment's prerequisites before the first run:
// PostgreSQL settings and privileges, sink reachability, and configuration
// consistency. It is wired to `gtc doctor` so a misconfigured environment is
// diagnosed in one command instead of by reading startup logs.
package doctor

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/meilisearch/meilisearch-go"
	"github.com/redis/go-redis/v9"

	meilisink "github.com/emoss08/gtc/internal/adapters/secondary/meilisearch"
	redissink "github.com/emoss08/gtc/internal/adapters/secondary/redis"
	redisjsonsink "github.com/emoss08/gtc/internal/adapters/secondary/redisjson"
	"github.com/emoss08/gtc/internal/infrastructure/config"
	"github.com/emoss08/gtc/internal/infrastructure/transform"
)

type status int

const (
	pass status = iota
	warn
	fail
	skip
)

func (s status) glyph() string {
	switch s {
	case pass:
		return "✓"
	case warn:
		return "!"
	case fail:
		return "✗"
	default:
		return "-"
	}
}

type result struct {
	name   string
	status status
	detail string
}

type report struct {
	out     io.Writer
	results []result
}

func (r *report) section(title string) { fmt.Fprintf(r.out, "\n%s\n", title) }

func (r *report) add(name string, st status, format string, args ...any) {
	res := result{name: name, status: st, detail: fmt.Sprintf(format, args...)}
	r.results = append(r.results, res)
	fmt.Fprintf(r.out, "  %s %-22s %s\n", st.glyph(), res.name, res.detail)
}

// Run executes every check and returns the process exit code: 0 when nothing
// failed (warnings allowed), 1 otherwise.
func Run(ctx context.Context, out io.Writer) int {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	r := &report{out: out}
	fmt.Fprintln(out, "gtc doctor — checking prerequisites")

	cfg := checkConfig(r)
	if cfg != nil {
		checkPostgres(ctx, r, cfg)
	}
	sinkCount := checkSinks(ctx, r, cfg)
	if cfg != nil {
		checkPipelineFit(ctx, r, cfg, sinkCount)
	}

	warns, fails := 0, 0
	for _, res := range r.results {
		switch res.status {
		case warn:
			warns++
		case fail:
			fails++
		}
	}
	fmt.Fprintf(out, "\n%d failure(s), %d warning(s)\n", fails, warns)
	if fails > 0 {
		fmt.Fprintln(out, "Fix the failures above before starting GTC.")
		return 1
	}
	fmt.Fprintln(out, "Ready to stream.")
	return 0
}

func checkConfig(r *report) *config.Config {
	r.section("Configuration")
	cfg, err := config.Load()
	if err != nil {
		r.add("environment", fail, "%v", err)
		return nil
	}
	r.add("environment", pass, "slot %q, publication %q", cfg.SlotName, cfg.PublicationName)
	return cfg
}

// plainURL strips replication=database so ordinary SQL can run; the gateway's
// DATABASE_URL carries it for the WAL session.
func plainURL(dbURL string) string {
	u, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	q := u.Query()
	q.Del("replication")
	u.RawQuery = q.Encode()
	return u.String()
}

func replicationURL(dbURL string) string {
	u, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String()
}

func checkPostgres(ctx context.Context, r *report, cfg *config.Config) {
	r.section("PostgreSQL")

	db, err := pgx.Connect(ctx, plainURL(cfg.DatabaseURL))
	if err != nil {
		r.add("connection", fail, "%v", err)
		return
	}
	defer db.Close(context.Background())

	var versionNum int
	var versionText string
	if err := db.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int, current_setting('server_version')",
	).Scan(&versionNum, &versionText); err != nil {
		r.add("connection", fail, "query server version: %v", err)
		return
	}
	if versionNum < 140000 {
		r.add("connection", warn,
			"PostgreSQL %s — GTC needs 14+ (pgoutput protocol v2)", versionText)
	} else {
		r.add("connection", pass, "PostgreSQL %s", versionText)
	}

	var walLevel string
	_ = db.QueryRow(ctx, "SELECT current_setting('wal_level')").Scan(&walLevel)
	if walLevel != "logical" {
		r.add("wal_level", fail,
			"%q — logical replication needs wal_level=logical "+
				"(ALTER SYSTEM SET wal_level = 'logical'; then restart PostgreSQL)", walLevel)
	} else {
		r.add("wal_level", pass, "logical")
	}

	var user string
	var super, repl bool
	if err := db.QueryRow(ctx,
		"SELECT current_user, rolsuper, rolreplication FROM pg_roles WHERE rolname = current_user",
	).Scan(&user, &super, &repl); err == nil {
		switch {
		case super:
			r.add("privileges", pass, "user %q is superuser", user)
		case repl:
			r.add("privileges", pass, "user %q has REPLICATION", user)
		default:
			r.add("privileges", warn,
				"user %q has neither SUPERUSER nor REPLICATION — "+
					"grant it (or rds_replication on AWS RDS); "+
					"the protocol check below is authoritative", user)
		}
	}

	var slotsMax, slotsUsed int
	if err := db.QueryRow(ctx,
		"SELECT current_setting('max_replication_slots')::int, (SELECT count(*) FROM pg_replication_slots)",
	).Scan(&slotsMax, &slotsUsed); err == nil {
		var slotActive *bool
		slotExists := db.QueryRow(ctx,
			"SELECT active FROM pg_replication_slots WHERE slot_name = $1",
			cfg.SlotName).Scan(&slotActive) == nil
		switch {
		case slotExists && slotActive != nil && *slotActive:
			r.add("replication slot", warn,
				"slot %q exists and is ACTIVE — is another GTC (or consumer) already attached?",
				cfg.SlotName)
		case slotExists:
			r.add("replication slot", pass, "slot %q exists (%d/%d slots in use)",
				cfg.SlotName, slotsUsed, slotsMax)
		case slotsUsed >= slotsMax:
			r.add("replication slot", fail,
				"all %d replication slots are in use and %q does not exist — "+
					"raise max_replication_slots or drop an unused slot", slotsMax, cfg.SlotName)
		case !cfg.AutoCreateSlot:
			r.add("replication slot", fail,
				"slot %q does not exist and CDC_AUTO_CREATE_SLOT is false", cfg.SlotName)
		default:
			r.add("replication slot", pass, "slot %q will be auto-created (%d/%d slots in use)",
				cfg.SlotName, slotsUsed, slotsMax)
		}
	}

	var sendersMax, sendersUsed int
	if err := db.QueryRow(ctx,
		"SELECT current_setting('max_wal_senders')::int, (SELECT count(*) FROM pg_stat_replication)",
	).Scan(&sendersMax, &sendersUsed); err == nil {
		if sendersUsed >= sendersMax {
			r.add("wal senders", fail, "all %d wal senders are busy — raise max_wal_senders", sendersMax)
		} else {
			r.add("wal senders", pass, "%d/%d in use", sendersUsed, sendersMax)
		}
	}

	var pubExists bool
	_ = db.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
		cfg.PublicationName).Scan(&pubExists)
	switch {
	case pubExists:
		r.add("publication", pass, "%q exists", cfg.PublicationName)
	case !cfg.AutoCreatePub:
		r.add("publication", fail,
			"%q does not exist and CDC_AUTO_CREATE_PUBLICATION is false", cfg.PublicationName)
	case !super:
		r.add("publication", fail,
			"%q does not exist and auto-creating it FOR ALL TABLES requires superuser — "+
				"create it manually: CREATE PUBLICATION %s FOR ALL TABLES", cfg.PublicationName,
			cfg.PublicationName)
	default:
		r.add("publication", pass, "%q will be auto-created", cfg.PublicationName)
	}

	// Published tables with default replica identity and no primary key:
	// PostgreSQL refuses UPDATE/DELETE on them once they are published.
	rows, err := db.Query(ctx, `
		SELECT n.nspname || '.' || c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND c.relreplident = 'd'
		  AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
		ORDER BY 1`)
	if err == nil {
		var noPK []string
		for rows.Next() {
			var t string
			if rows.Scan(&t) == nil {
				noPK = append(noPK, t)
			}
		}
		rows.Close()
		if len(noPK) > 0 {
			shown := noPK
			if len(shown) > 5 {
				shown = shown[:5]
			}
			r.add("replica identity", warn,
				"%d table(s) without a primary key or replica identity — "+
					"UPDATE/DELETE on them will fail once published: %s",
				len(noPK), strings.Join(shown, ", "))
		} else {
			r.add("replica identity", pass, "every table has a primary key or replica identity")
		}
	}

	// The authoritative test: open a real replication session.
	repConn, err := pgconn.Connect(ctx, replicationURL(cfg.DatabaseURL))
	if err != nil {
		r.add("replication protocol", fail, "%v", err)
		return
	}
	defer repConn.Close(context.Background())
	ident, err := pglogrepl.IdentifySystem(ctx, repConn)
	if err != nil {
		r.add("replication protocol", fail, "IDENTIFY_SYSTEM: %v", err)
		return
	}
	r.add("replication protocol", pass, "IDENTIFY_SYSTEM ok (timeline %d, %s)",
		ident.Timeline, ident.XLogPos)
}

// checkSinks probes each configured sink and returns how many are enabled.
func checkSinks(ctx context.Context, r *report, cfg *config.Config) int {
	r.section("Sinks")

	sinksCfg, err := config.LoadSinksConfig()
	if err != nil {
		r.add("sink config", fail, "%v", err)
		sinksCfg = nil
	} else if sinksCfg != nil && cfg != nil {
		checkTransforms(r, cfg, sinksCfg)
	}

	count := 0

	if rc := redissink.LoadConfig(); rc.Enabled {
		count++
		checkRedis(ctx, r, "redis stream", rc.URL, false)
	} else {
		r.add("redis stream", skip, "not configured (REDIS_URL unset)")
	}

	if jc := redisjsonsink.LoadConfig(); jc.Enabled {
		count++
		checkRedis(ctx, r, "redisjson", jc.URL, true)
	} else {
		r.add("redisjson", skip, "not configured (REDIS_JSON_URL unset)")
	}

	if mc := meilisink.LoadConfig(); mc.Enabled {
		count++
		client := meilisearch.New(mc.URL, meilisearch.WithAPIKey(mc.APIKey))
		if _, err := client.HealthWithContext(ctx); err != nil {
			r.add("meilisearch", fail, "%v", err)
		} else {
			r.add("meilisearch", pass, "healthy at %s", mc.URL)
		}
	} else {
		r.add("meilisearch", skip, "not configured (MEILISEARCH_URL unset)")
	}

	if count == 0 {
		r.add("sinks", warn, "no sinks configured — GTC will consume WAL but deliver nowhere")
	}

	if cfg != nil && cfg.DLQ.Enabled && redissink.LoadConfig().URL == "" {
		r.add("dead-letter queue", warn,
			"CDC_DLQ_ENABLED is true but REDIS_URL is unset — the DLQ is unavailable "+
				"and poison events will stall the pipeline")
	}

	if sinksCfg != nil && sinksCfg.Outbox.Enabled() && redissink.LoadConfig().URL == "" {
		r.add("outbox", fail, "outbox is configured but REDIS_URL is unset")
	}

	return count
}

func checkRedis(ctx context.Context, r *report, name, rawURL string, needJSON bool) {
	// go-redis logs every dial failure to stderr; the report line is enough.
	redis.SetLogger(discardLogger{})
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		r.add(name, fail, "invalid URL: %v", err)
		return
	}
	opts.MaxRetries = -1 // fail fast; doctor reports, it doesn't recover
	client := redis.NewClient(opts)
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		r.add(name, fail, "%v", err)
		return
	}
	if needJSON {
		// JSON.TYPE on a missing key answers nil when the RedisJSON module
		// is loaded and "unknown command" when it is not.
		if err := client.Do(ctx, "JSON.TYPE", "gtc:doctor:probe").Err(); err != nil &&
			!strings.Contains(err.Error(), "redis: nil") && err != redis.Nil {
			r.add(name, fail, "reachable, but the RedisJSON module is missing (%v)", err)
			return
		}
	}
	r.add(name, pass, "PONG from %s", opts.Addr)
}

type discardLogger struct{}

func (discardLogger) Printf(context.Context, string, ...any) {}

// checkTransforms compiles every transform spec exactly as startup would.
func checkTransforms(r *report, cfg *config.Config, sinksCfg *config.SinksConfig) {
	opts := transform.Options{HMACKey: []byte(cfg.MaskHMACKey)}

	compile := func(scope string, global *config.TransformSpec, tables map[string]config.TransformSpec) bool {
		if global != nil {
			if _, err := transform.CompileWithOptions(opts, *global); err != nil {
				r.add("transforms", fail, "%s: %v", scope, err)
				return false
			}
		}
		for name, spec := range tables {
			if _, err := transform.CompileWithOptions(opts, spec); err != nil {
				r.add("transforms", fail, "%s, table %s: %v", scope, name, err)
				return false
			}
		}
		return true
	}

	ok := compile("redis_stream", sinksCfg.RedisStream.Transform, sinksCfg.RedisStream.TableTransforms()) &&
		compile("redis_json", sinksCfg.RedisJSON.Transform, sinksCfg.RedisJSON.TableTransforms()) &&
		compile("meilisearch", sinksCfg.Meilisearch.Transform, sinksCfg.Meilisearch.TableTransforms())
	if ok {
		r.add("transforms", pass, "all filters and masks compile")
	}
}

// checkPipelineFit warns when sink timeouts could starve standby keepalives:
// sinks run synchronously with the WAL stream, so the worst case is every
// sink timing out on one event back to back.
func checkPipelineFit(ctx context.Context, r *report, cfg *config.Config, sinkCount int) {
	r.section("Pipeline")

	db, err := pgx.Connect(ctx, plainURL(cfg.DatabaseURL))
	if err != nil {
		return // already reported under PostgreSQL
	}
	defer db.Close(context.Background())

	var senderTimeoutMS int
	if err := db.QueryRow(ctx,
		"SELECT floor(extract(epoch from current_setting('wal_sender_timeout')::interval) * 1000)::int",
	).Scan(&senderTimeoutMS); err != nil {
		return
	}
	if senderTimeoutMS == 0 {
		r.add("timeouts", pass, "wal_sender_timeout is disabled")
		return
	}

	senderTimeout := time.Duration(senderTimeoutMS) * time.Millisecond
	worstCase := time.Duration(max(sinkCount, 1)) * cfg.ProcessTimeout
	if worstCase >= senderTimeout {
		r.add("timeouts", warn,
			"%d sink(s) x CDC_PROCESS_TIMEOUT (%s) = %s, which can exceed wal_sender_timeout (%s) "+
				"and get the replication connection dropped mid-event — lower CDC_PROCESS_TIMEOUT "+
				"or raise wal_sender_timeout",
			max(sinkCount, 1), cfg.ProcessTimeout, worstCase, senderTimeout)
	} else {
		r.add("timeouts", pass, "%d sink(s) x CDC_PROCESS_TIMEOUT (%s) fits within wal_sender_timeout (%s)",
			max(sinkCount, 1), cfg.ProcessTimeout, senderTimeout)
	}
}

package mongo

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type OfflineProbeConfig struct {
	MongodPath string
	DBPath     string
	Bind       string
	Port       int
	AdminUser  string
	AdminPass  string
}

// Probe starts a temporary local mongod WITHOUT replSet and WITHOUT auth,
// reads local.system.replset + local.oplog.rs (if exists), removes replSet doc,
// and ensures an admin user exists (so later shutdown with auth can be used if you enable auth in your main runtime).
//
// This is intentionally conservative:
// - It binds only to 127.0.0.1
// - It uses a short timeout
func Probe(ctx context.Context, cfg OfflineProbeConfig) (replSetID string, replSetUUID string, term int64, lastOplog time.Time, err error) {
	if cfg.MongodPath == "" || cfg.DBPath == "" || cfg.Port == 0 {
		return "", "", 0, time.Time{}, errors.New("mongod_path, dbpath, temp_port required")
	}

	bind := cfg.Bind
	if bind == "" {
		bind = "127.0.0.1"
	}

	if err := waitPortFree(bind, cfg.Port); err != nil {
		return "", "", 0, time.Time{}, fmt.Errorf("temp port not free: %w", err)
	}

	// Start mongod
	args := []string{
		"--dbpath", cfg.DBPath,
		"--port", fmt.Sprintf("%d", cfg.Port),
		"--bind_ip", bind,
		"--logpath", filepath.Join(cfg.DBPath, "replctl-offline-probe.log"),
	}
	log.Printf("[mongo-worker] offline probe mongod command: %s %s", cfg.MongodPath, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, cfg.MongodPath, args...)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return "", "", 0, time.Time{}, err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		scan := func(r *bufio.Scanner) {
			for r.Scan() {
				// keep logs light
			}
		}
		if stdout != nil {
			scan(bufio.NewScanner(stdout))
		}
		if stderr != nil {
			scan(bufio.NewScanner(stderr))
		}
	}()

	// connect
	uri := fmt.Sprintf("mongodb://%s/?directConnection=true", net.JoinHostPort(bind, fmt.Sprintf("%d", cfg.Port)))
	cli, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		_ = stopProcess(cmd, done)
		return "", "", 0, time.Time{}, err
	}
	defer func() { _ = cli.Disconnect(context.Background()) }()

	// wait ping
	pingCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		if err := cli.Database("admin").RunCommand(pingCtx, bson.D{{Key: "ping", Value: 1}}).Err(); err == nil {
			break
		}
		if pingCtx.Err() != nil {
			_ = stopProcess(cmd, done)
			return "", "", 0, time.Time{}, fmt.Errorf("mongod not ready: %w", pingCtx.Err())
		}
		time.Sleep(300 * time.Millisecond)
	}

	// read local.system.replset
	localDB := cli.Database("local")
	var replDoc bson.M
	err = localDB.Collection("system.replset").FindOne(ctx, bson.D{}).Decode(&replDoc)
	if err == nil {
		replSetID = extractReplSetID(replDoc)
		replSetUUID = extractReplSetUUID(replDoc)
	}

	// read last oplog entry
	cur, err2 := localDB.Collection("oplog.rs").Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetLimit(1))
	if err2 == nil {
		defer cur.Close(ctx)
		if cur.Next(ctx) {
			var doc bson.M
			_ = cur.Decode(&doc)
			// ts is bson.Timestamp {T, I}
			if ts, ok := doc["ts"].(primitive.Timestamp); ok {
				lastOplog = time.Unix(int64(ts.T), 0)
			}
			if t, ok := doc["t"].(int64); ok { // term may be stored as "t" depending on version
				term = t
			}
			if term == 0 {
				if t32, ok := doc["t"].(int32); ok {
					term = int64(t32)
				}
			}
		}
	}

	// ensure admin user exists (best-effort)
	if cfg.AdminUser != "" && cfg.AdminPass != "" {
		ensureAdminUser(ctx, cli, cfg.AdminUser, cfg.AdminPass)
	}

	// remove replset config document (best-effort) to allow standalone start later
	//_, _ = localDB.Collection("system.replset").DeleteMany(ctx, bson.D{})

	_ = stopProcess(cmd, done)
	return replSetID, replSetUUID, term, lastOplog, nil
}

func ensureAdminUser(ctx context.Context, cli *mongo.Client, user, pass string) {
	// check usersInfo
	var res bson.M
	_ = cli.Database("admin").RunCommand(ctx, bson.D{
		{Key: "usersInfo", Value: bson.D{{Key: "user", Value: user}, {Key: "db", Value: "admin"}}},
	}).Decode(&res)
	// If already exists, do nothing.
	if users, ok := res["users"].(bson.A); ok && len(users) > 0 {
		return
	}
	// create user
	err := cli.Database("admin").RunCommand(ctx, bson.D{
		{Key: "createUser", Value: user},
		{Key: "pwd", Value: pass},
		{Key: "roles", Value: bson.A{
			bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}},
		}},
	}).Err()
	if err != nil {
		log.Printf("[mongo-worker] createUser failed: %v", err)
	}
}

func extractReplSetID(doc bson.M) string {
	if id, ok := doc["_id"].(string); ok && id != "" {
		return id
	}
	if cfg, ok := doc["config"].(bson.M); ok {
		if id, ok := cfg["_id"].(string); ok && id != "" {
			return id
		}
	}
	return ""
}

func extractReplSetUUID(doc bson.M) string {
	if u := uuidFromVal(doc["replicaSetId"]); u != "" {
		return u
	}
	if u := uuidFromVal(doc["uuid"]); u != "" {
		return u
	}
	if cfg, ok := doc["config"].(bson.M); ok {
		if u := uuidFromVal(cfg["replicaSetId"]); u != "" {
			return u
		}
		if u := uuidFromVal(cfg["uuid"]); u != "" {
			return u
		}
	}
	if settings, ok := doc["settings"].(bson.M); ok {
		if u := uuidFromVal(settings["replicaSetId"]); u != "" {
			return u
		}
	}
	return ""
}

func uuidFromVal(v any) string {
	switch t := v.(type) {
	case primitive.Binary:
		if len(t.Data) == 16 {
			return formatUUID(t.Data)
		}
		if len(t.Data) > 0 {
			return hex.EncodeToString(t.Data)
		}
	case primitive.ObjectID:
		return t.Hex()
	case string:
		return t
	}
	return ""
}

func formatUUID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func waitPortFree(bind string, port int) error {
	l, err := net.Listen("tcp", net.JoinHostPort(bind, fmt.Sprintf("%d", port)))
	if err != nil {
		return err
	}
	_ = l.Close()
	return nil
}

func stopProcess(cmd *exec.Cmd, done <-chan struct{}) error {
	// try graceful
	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case <-done:
		return nil
	case <-time.After(2 * time.Second):
	}
	_ = cmd.Process.Kill()
	select {
	case <-done:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("mongod probe process did not exit")
	}
}

// Diagnostic probe for DingTalk wiki data sources: decrypts a data source's
// stored credentials with the running app's SYSTEM_AES_KEY, lists the wiki
// tree exactly like the sync connector does, and prints the node-type
// distribution so "synced 0 documents" becomes explainable (empty space,
// permission, or non-adoc types the connector skips).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/dingtalk"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

func main() {
	dsID := flag.String("ds", "", "data source id")
	parent := flag.String("parent", "", "parent node id (defaults to the first selected resource)")
	flag.Parse()
	if *dsID == "" {
		fmt.Println("-ds is required")
		os.Exit(2)
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"))
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		fatal("open db: %v", err)
	}

	var ds types.DataSource
	if err := db.Where("id = ?", *dsID).First(&ds).Error; err != nil {
		fatal("load datasource: %v", err)
	}
	raw, err := ds.ParseConfig()
	if err != nil {
		fatal("parse config: %v", err)
	}
	if raw == nil {
		fatal("datasource has no config")
	}

	target := *parent
	if target == "" && len(raw.ResourceIDs) > 0 {
		target = raw.ResourceIDs[0]
	}
	fmt.Printf("datasource=%s type=%s target_parent=%s credentials=%v\n",
		ds.ID, ds.Type, target, raw.HasCredentials())

	connector := dingtalk.NewConnector()
	if rawNodes, nerr := connector.ListRawNodes(context.Background(), raw, strings.TrimPrefix(
		strings.TrimPrefix(target, "dingtalk:v1?node="), "")); nerr == nil {
		fmt.Printf("raw children of %s: %d\n", target, len(rawNodes))
		mix := map[string]int{}
		for _, n := range rawNodes {
			key := n.Type + "/" + n.Category + "/" + n.Extension
			mix[key]++
		}
		for k, v := range mix {
			fmt.Printf("  type-mix %-40s x%d\n", k, v)
		}
	} else {
		fmt.Printf("raw listing failed: %v\n", nerr)
	}
	resources, err := connector.ListResources(context.Background(), raw, target)
	if err != nil {
		fatal("list resources: %v", err)
	}
	fmt.Printf("visible resources under target: %d\n", len(resources))
	for i, r := range resources {
		if i >= 20 {
			fmt.Println("  …")
			break
		}
		fmt.Printf("  %s | %s | %s\n", r.ExternalID, r.Type, r.Name)
	}
	fmt.Println("probe done", time.Now().UTC().Format(time.RFC3339))
}

func fatal(format string, args ...any) {
	fmt.Println("ERROR:", fmt.Sprintf(format, args...))
	os.Exit(1)
}

package repository

import (
	"fmt"
	"gorm.io/gorm"
	"strings"
)

func bodyQuoted(id string) string        { return `"` + strings.ReplaceAll(id, `"`, `""`) + `"` }
func bodySQLLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
func originalBodyColumnIsJSON(tx *gorm.DB, table, col string) (bool, error) {
	columns, e := bodyInternal(tx).Migrator().ColumnTypes(table)
	if e != nil {
		return false, e
	}
	for _, c := range columns {
		if c.Name() == bodyFieldColumn(col) {
			name := strings.ToLower(c.DatabaseTypeName())
			return strings.Contains(name, "json"), nil
		}
	}
	return false, nil
}
func originalBodyRewrite(tx *gorm.DB, d originalBodyFieldDescriptor, key, col, id, old, new string) error {
	internal := bodyInternal(tx)
	if strings.Contains(col, ".") {
		row, e := bodySourceRow(tx, d, key)
		if e != nil {
			return e
		}
		stored := bodyText(row[bodyFieldColumn(col)])
		updated, e := bodyNestedReplace(stored, col, old, new)
		if e != nil {
			return e
		}
		if tx.Dialector.Name() == "postgres" {
			if e := internal.Raw("SELECT CAST(? AS jsonb)::text", updated).Scan(&updated).Error; e != nil {
				return e
			}
		}
		old, new, col = stored, updated, bodyFieldColumn(col)
	}
	if e := internal.Exec("INSERT INTO original_body_rewrite_authorities(source_table,row_key,column_name,body_id,old_value,new_value) VALUES(?,?,?,?,?,?)", d.Table, key, col, id, old, new).Error; e != nil {
		return e
	}
	query := "UPDATE " + bodyQuoted(d.Table) + " SET " + bodyQuoted(col) + "=? WHERE CAST(" + bodyQuoted(d.Key) + " AS TEXT)=?"
	result := internal.Exec(query, new, key)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrOriginalBodyUnavailable
	}
	return internal.Exec("DELETE FROM original_body_rewrite_authorities WHERE source_table=? AND row_key=? AND column_name=?", d.Table, key, col).Error
}

// Only a same-transaction, exact old bytes -> exact reference/empty-body move
// can bypass an immutable or semantic version trigger. All metadata columns
// and every other body field must compare equal. Delete triggers are preserved.
func InstallOriginalBodyRetentionTriggers(db *gorm.DB) error {
	if !db.Migrator().HasTable(&originalBodyPayload{}) {
		return nil
	}
	if db.Dialector.Name() == "postgres" {
		return installPostgresBodyRewrite(db)
	}
	if db.Dialector.Name() != "sqlite" {
		return ErrOriginalBodyUnavailable
	}
	for _, d := range originalBodyFields {
		if !db.Migrator().HasTable(d.Table) {
			continue
		}
		columns, e := bodyInternal(db).Migrator().ColumnTypes(d.Table)
		if e != nil {
			return e
		}
		known := map[string]bool{}
		for _, c := range columns {
			known[c.Name()] = true
		}
		permitted := []string{}
		for _, field := range d.Columns {
			col := bodyFieldColumn(field)
			if !known[col] {
				continue
			}
			eq := []string{}
			for _, other := range columns {
				if other.Name() != col {
					eq = append(eq, "NEW."+bodyQuoted(other.Name())+" IS OLD."+bodyQuoted(other.Name()))
				}
			}
			a := "EXISTS (SELECT 1 FROM original_body_rewrite_authorities a JOIN original_body_payloads p ON p.id=a.body_id WHERE a.source_table=" + bodySQLLiteral(d.Table) + " AND a.row_key=CAST(OLD." + bodyQuoted(d.Key) + " AS TEXT) AND a.column_name=" + bodySQLLiteral(col) + " AND a.old_value=OLD." + bodyQuoted(col) + " AND a.new_value=NEW." + bodyQuoted(col) + ")"
			permitted = append(permitted, "("+strings.Join(eq, " AND ")+" AND "+a+")")
		}
		if len(permitted) == 0 {
			continue
		}
		permit := strings.Join(permitted, " OR ")
		var triggers []struct{ Name, SQL string }
		if e := bodyInternal(db).Raw("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?", d.Table).Scan(&triggers).Error; e != nil {
			return e
		}
		for _, trigger := range triggers {
			upper := strings.ToUpper(trigger.SQL)
			if strings.Contains(upper, "ORIGINAL_BODY_REWRITE_AUTHORITIES") || !strings.Contains(upper, "UPDATE ON") && !strings.Contains(upper, "UPDATE OF ") {
				continue
			}
			begin := strings.Index(upper, " BEGIN")
			if begin < 0 {
				begin = strings.Index(upper, "\nBEGIN")
			}
			if begin < 0 {
				return fmt.Errorf("unsupported original body trigger syntax")
			}
			prefix, suffix := trigger.SQL[:begin], trigger.SQL[begin:]
			pUpper := strings.ToUpper(prefix)
			when := strings.Index(pUpper, " WHEN ")
			if when < 0 {
				when = strings.Index(pUpper, "\nWHEN ")
			}
			if when >= 0 {
				oldCondition := strings.TrimSpace(prefix[when+6:])
				prefix = prefix[:when] + " WHEN (" + oldCondition + ") AND NOT (" + permit + ")"
			} else {
				prefix += " WHEN NOT (" + permit + ")"
			}
			if e := bodyInternal(db).Transaction(func(tx *gorm.DB) error {
				if e := tx.Exec("DROP TRIGGER " + bodyQuoted(trigger.Name)).Error; e != nil {
					return e
				}
				return tx.Exec(prefix + suffix).Error
			}); e != nil {
				return e
			}
		}
	}
	return nil
}
func installPostgresBodyRewrite(db *gorm.DB) error {
	// Replacing one trigger function preserves every existing trigger binding.
	// A wrapper calls the original function for all non-protocol writes; exact
	// physical field moves return NEW and leave semantic counters unchanged.
	for _, d := range originalBodyFields {
		if !db.Migrator().HasTable(d.Table) {
			continue
		}
		columns, e := bodyInternal(db).Migrator().ColumnTypes(d.Table)
		if e != nil {
			return e
		}
		known := map[string]bool{}
		for _, c := range columns {
			known[c.Name()] = true
		}
		conditions := []string{}
		for _, field := range d.Columns {
			col := bodyFieldColumn(field)
			if !known[col] {
				continue
			}
			// to_jsonb(row)->> handles both text and canonical json/jsonb values.
			conditions = append(conditions, "((to_jsonb(NEW)-"+bodySQLLiteral(col)+")=(to_jsonb(OLD)-"+bodySQLLiteral(col)+") AND EXISTS(SELECT 1 FROM original_body_rewrite_authorities a JOIN original_body_payloads p ON p.id=a.body_id WHERE a.source_table="+bodySQLLiteral(d.Table)+" AND a.row_key=to_jsonb(OLD)->>"+bodySQLLiteral(d.Key)+" AND a.column_name="+bodySQLLiteral(col)+" AND a.old_value=COALESCE(to_jsonb(OLD)->>"+bodySQLLiteral(col)+",'null') AND a.new_value=COALESCE(to_jsonb(NEW)->>"+bodySQLLiteral(col)+",'null')))")
		}
		if len(conditions) == 0 {
			continue
		}
		var triggers []struct{ Name, Definition, Function string }
		q := `SELECT t.tgname AS name,pg_get_triggerdef(t.oid) AS definition,pg_get_functiondef(t.tgfoid) AS function FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname=? AND n.nspname=current_schema() AND NOT t.tgisinternal`
		if e := bodyInternal(db).Raw(q, d.Table).Scan(&triggers).Error; e != nil {
			return e
		}
		for _, trigger := range triggers {
			if strings.Contains(trigger.Function, "original_body_rewrite_authorities") || !strings.Contains(trigger.Definition, "UPDATE") {
				continue
			}
			// Clone the exact original trigger body to a per-trigger wrapper; inserting
			// the narrow guard before BEGIN cannot affect the original policy branches.
			begin := strings.Index(strings.ToUpper(trigger.Function), "BEGIN")
			if begin < 0 {
				return fmt.Errorf("unsupported original body trigger language")
			}
			start := strings.Index(trigger.Function, "$function$")
			end := strings.LastIndex(trigger.Function, "$function$")
			if start < 0 || end <= start {
				return fmt.Errorf("unsupported original body function quoting")
			}
			original := trigger.Function[start+len("$function$") : end]
			localBegin := strings.Index(strings.ToUpper(original), "BEGIN")
			if localBegin < 0 {
				return fmt.Errorf("unsupported original body trigger language")
			}
			guarded := original[:localBegin] + "BEGIN IF TG_OP='UPDATE' AND (" + strings.Join(conditions, " OR ") + ") THEN RETURN NEW; END IF;" + original[localBegin+len("BEGIN"):]
			name := "original_body_" + bodySHA([]byte(d.Table + "/" + trigger.Name))[:20]
			sql := "CREATE OR REPLACE FUNCTION " + bodyQuoted(name) + "() RETURNS trigger LANGUAGE plpgsql AS $body$ " + guarded + " $body$"
			if e := bodyInternal(db).Exec(sql).Error; e != nil {
				return e
			}
			definition := trigger.Definition
			exec := strings.LastIndex(definition, " EXECUTE ")
			if exec < 0 {
				return fmt.Errorf("unsupported original body trigger binding")
			}
			definition = definition[:exec] + " EXECUTE FUNCTION " + bodyQuoted(name) + "()"
			if e := bodyInternal(db).Transaction(func(tx *gorm.DB) error {
				if e := tx.Exec("DROP TRIGGER " + bodyQuoted(trigger.Name) + " ON " + bodyQuoted(d.Table)).Error; e != nil {
					return e
				}
				return tx.Exec(definition).Error
			}); e != nil {
				return e
			}
		}
	}
	return nil
}

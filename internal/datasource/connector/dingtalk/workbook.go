package dingtalk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Each GetRange call returns a rectangle of formatted cell strings. Limit both
// each request and the total work for sparse workbooks with distant cells.
const workbookRangeCells = 1000

func (c *client) workbookMarkdown(ctx context.Context, document node) (string, error) {
	path := "/v1.0/doc/workbooks/" + url.PathEscape(document.ID) + "/sheets"
	query := "?operatorId=" + url.QueryEscape(c.operator)
	var list struct {
		Value []struct{ ID, Name string }
	}
	if err := c.doJSON(ctx, http.MethodGet, path+query, nil, true, &list); err != nil {
		return "", fmt.Errorf("list DingTalk worksheets (Document.Workbook.Read): %w", err)
	}
	if len(list.Value) == 0 || len(list.Value) > maxPages {
		return "", errors.New("empty or oversized DingTalk worksheet list")
	}
	var out strings.Builder
	fmt.Fprintf(&out, "# %s\n", document.title())
	var requests int
	for _, sheet := range list.Value {
		if sheet.ID == "" {
			return "", errors.New("DingTalk worksheet missing ID")
		}
		sheetPath := path + "/" + url.PathEscape(sheet.ID)
		var meta struct {
			LastNonEmptyRow, LastNonEmptyColumn *int
			RowCount, ColumnCount               *int
		}
		if err := c.doJSON(ctx, http.MethodGet, sheetPath+query, nil, true, &meta); err != nil {
			return "", fmt.Errorf("read DingTalk worksheet dimensions: %w", err)
		}
		// GetSheet normally reports zero-based last occupied indexes. When
		// omitted, scan the reported grid under the same range/size limits.
		rows, err := worksheetExtent(meta.LastNonEmptyRow, meta.RowCount)
		if err != nil {
			return "", err
		}
		cols, err := worksheetExtent(meta.LastNonEmptyColumn, meta.ColumnCount)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&out, "\n## %s\n", sheet.Name)
		if out.Len() > maxResponseBytes {
			return "", errors.New("DingTalk workbook text exceeds size limit")
		}
		if rows == 0 || cols == 0 {
			continue
		}
		lastRow, lastCol := rows-1, cols-1
		width := min(lastCol+1, workbookRangeCells)
		height := workbookRangeCells / width
		for row := 0; row <= lastRow; row += height {
			for col := 0; col <= lastCol; col += width {
				requests++
				if requests > maxPages {
					return "", errors.New("DingTalk workbook exceeds range request limit")
				}
				rows, cols := min(height, lastRow-row+1), min(width, lastCol-col+1)
				address := fmt.Sprintf("%s%d:%s%d", columnName(col), row+1, columnName(col+cols-1), row+rows)
				var block struct{ DisplayValues [][]string }
				if err := c.doJSON(ctx, http.MethodGet, sheetPath+"/ranges/"+address+query+"&select=displayValues",
					nil, true, &block); err != nil {
					return "", fmt.Errorf("read DingTalk worksheet cells: %w", err)
				}
				if len(block.DisplayValues) != rows {
					return "", errors.New("incomplete row data in DingTalk worksheet range")
				}
				for i, cells := range block.DisplayValues {
					if len(cells) != cols {
						return "", errors.New("incomplete column data in DingTalk worksheet range")
					}
					for j, value := range cells {
						if value != "" {
							fmt.Fprintf(&out, "%s%d: %s\n", columnName(col+j), row+i+1, value)
						}
					}
					if out.Len() > maxResponseBytes {
						return "", errors.New("DingTalk workbook text exceeds size limit")
					}
				}
			}
		}
	}
	return out.String(), nil
}

func worksheetExtent(last, count *int) (int, error) {
	const limit = maxPages * workbookRangeCells
	if last != nil && *last >= -1 && *last < limit {
		return *last + 1, nil
	}
	if last == nil && count != nil && *count >= 0 && *count <= limit {
		return *count, nil
	}
	return 0, errors.New("missing, invalid or oversized DingTalk worksheet dimensions")
}

func columnName(col int) string {
	var name string
	for n := col + 1; n > 0; n = (n - 1) / 26 {
		name = string(rune('A'+(n-1)%26)) + name
	}
	return name
}

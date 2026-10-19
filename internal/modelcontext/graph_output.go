package modelcontext

import (
	"fmt"
	"strings"
)

// Actual graph material and its endpoint chunks are both model inputs. The
// enclosing ToolResult carries the exhaustive private writer-input union;
// citation handles describe displayed sources and never replace that receipt.
func (r *sourceRegistry) modelKnowledgeGraphOutput(data map[string]interface{}, fallback string) string {
	graph, ok := data["graph"].(map[string]interface{})
	if !ok {
		return r.CompactKnownText(fallback)
	}
	var out strings.Builder
	out.WriteString("<knowledge_graph>\n")
	for _, node := range mapsValue(graph["nodes"]) {
		name := stringValue(node, "name")
		fmt.Fprintf(&out, "  <entity name=\"%s\">\n", escapeAttr(name))
		for _, attribute := range stringSliceValue(node["attributes"]) {
			fmt.Fprintf(&out, "    <attribute>%s</attribute>\n", escapeText(attribute))
		}
		out.WriteString("  </entity>\n")
	}
	for _, edge := range mapsValue(graph["edges"]) {
		fmt.Fprintf(
			&out,
			"  <relation source=\"%s\" target=\"%s\" type=\"%s\"/>\n",
			escapeAttr(stringValue(edge, "source")),
			escapeAttr(stringValue(edge, "target")),
			escapeAttr(stringValue(edge, "type")),
		)
	}
	out.WriteString("</knowledge_graph>\n")
	out.WriteString(r.modelKnowledgeOutput("graph", mapsValue(data["results"]), ""))
	return out.String()
}

package types

// DataSourceNameInputProof identifies the private immutable original data source name.
//
// A private immutable name receipt is reconstructed only by trusted SQL.
// Client complete flags or a current DataSource.Name are not an origin.
type DataSourceNameInputProof struct {
	ID, DataSourceID, KnowledgeBaseID, Name, BodyDigest, InputDigest string
	TenantID, Revision                                               uint64
}

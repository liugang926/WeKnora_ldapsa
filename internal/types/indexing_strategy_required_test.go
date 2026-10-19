package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequiredGraphConfigurationCannotDisableItsWriter(t *testing.T) {
	require.Error(t, (IndexingStrategy{GraphRequired: true}).ValidateGraphRequirement())
	require.Error(t, (IndexingStrategy{GraphRequired: true, VectorEnabled: true}).ValidateGraphRequirement())
	require.NoError(t, (IndexingStrategy{GraphRequired: true, GraphEnabled: true}).ValidateGraphRequirement())
	require.NoError(t, DefaultIndexingStrategy().ValidateGraphRequirement())
}

package types

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRequiredGraphConfigurationCannotDisableItsWriter(t *testing.T) {
	require.Error(t, (IndexingStrategy{GraphRequired: true}).ValidateGraphRequirement())
	require.Error(t, (IndexingStrategy{GraphRequired: true, VectorEnabled: true}).ValidateGraphRequirement())
	require.NoError(t, (IndexingStrategy{GraphRequired: true, GraphEnabled: true}).ValidateGraphRequirement())
	require.NoError(t, DefaultIndexingStrategy().ValidateGraphRequirement())
}

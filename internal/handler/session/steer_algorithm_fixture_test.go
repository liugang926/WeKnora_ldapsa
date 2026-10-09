package session

// These synthetic stores exercise the legacy queue algorithms only. Actual
// production repositories/services cannot implement this package-private port.
func (*steerMessageLookupStub) steerAlgorithmFixture()     {}
func (*steerPersistingMessageStub) steerAlgorithmFixture() {}
func (*steerFailingCreateStub) steerAlgorithmFixture()     {}

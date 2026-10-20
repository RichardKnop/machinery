package config

// UnmarshalYAML accepts the historical heartbeat spelling while preferring the
// correctly spelled key, even when it explicitly disables the heartbeat.
func (cnf *SQSConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain SQSConfig
	decoded := plain(*cnf)
	if err := unmarshal(&decoded); err != nil {
		return err
	}
	var keys struct {
		Heartbeat       *bool `yaml:"visibility_heartbeat"`
		LegacyHeartbeat *bool `yaml:"visibility_hearth_beat"`
	}
	if err := unmarshal(&keys); err != nil {
		return err
	}
	if keys.Heartbeat == nil && keys.LegacyHeartbeat != nil {
		decoded.VisibilityHeartBeat = *keys.LegacyHeartbeat
	}
	*cnf = SQSConfig(decoded)
	return nil
}

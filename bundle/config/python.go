package config

type Python struct {
	Where   string   `json:"where,omitempty"`
	Include []string `json:"include,omitempty"`
}

package v1

type Request struct {
	Image       string             `json:"code"`
	ModelWeight string             `json:"model_weight,omitempty"`
	ModelName   string             `json:"model_name,omitempty"`
	DataID      string             `json:"data_id,omitempty"`
	Resources   map[string]float64 `json:"resources"`
	Meta        map[string]string  `json:"meta,omitempty"`
}

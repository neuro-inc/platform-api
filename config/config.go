package config

// Config contains description of app configured variables
// TODO: frankly, it's too excess way to describe configuration
// I'd prefer a way which stnd `flag` package provides
type Config struct {
	ListenAddr string `default:":8080"`
}

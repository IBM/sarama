package kafka

type api interface {
	key() int16
	version() int16
}

type apiEncoder interface {
	encoder
	api
}

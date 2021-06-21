module sbplayground

go 1.16

require (
	github.com/Azure/azure-service-bus-go v0.10.12
	github.com/Azure/go-autorest/autorest v0.11.19 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.14 // indirect
	github.com/form3tech-oss/jwt-go v3.2.3+incompatible // indirect
	github.com/gofrs/uuid v4.0.0+incompatible
	github.com/klauspost/compress v1.13.0 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/rs/zerolog v1.23.0
	golang.org/x/crypto v0.0.0-20210513164829-c07d793c2f9a // indirect
	gopkg.in/tomb.v2 v2.0.0-20161208151619-d5d1b5820637
	nhooyr.io/websocket v1.8.7 // indirect
)

replace (
	// github.com/Azure/azure-service-bus-go => github.com/jhendrixMSFT/azure-service-bus-go nil_sessionid
	github.com/Azure/azure-service-bus-go => github.com/jhendrixMSFT/azure-service-bus-go v0.10.2-0.20210618212838-7ab4d5b8ae63
	// github.com/Azure/go-amqp v0.13.7 => github.com/jhendrixMSFT/go-amqp linksourcefilter
	github.com/Azure/go-amqp v0.13.7 => github.com/jhendrixMSFT/go-amqp v0.12.7-0.20210617221052-6e466f4fba8a
)

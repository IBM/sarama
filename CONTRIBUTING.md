# Contributing

Contributions are always welcome, both reporting issues and submitting pull requests!

### Reporting issues

- What sha of Sarama are you running? If this is not the latest sha on the master branch, please try if the problem persists with the latest version.
- You can set `sarama.Logger` to a [log.Logger](http://golang.org/pkg/log/#Logger) instance to capture debug output. Please include it in your issue description.

Also, please include the following information about your environment, so we can help you faster:

- What version of Kafka are you using?
- What version of Go are you using?
- What are the values of your Producer/Consumer/Client configuration?


### Submitting pull requests

- Make sure to use the `go fmt` command to format your code according to the standards. Even better, set up your editor to do this for you when saving.
- Run [go vet](https://godoc.org/golang.org/x/tools/cmd/vet) to detect any suspicious constructs in your code that could be bugs.
- Explicitly handle all error return values. If you really want to ignore an error value, you can assign it to `_`.You can use [errcheck](https://github.com/kisielk/errcheck) to verify whether you have handled all errors.
- You may also want to run [golint](https://github.com/golang/lint) as well to detect style problems.
- Add tests that cover the changes you made. Make sure to run `go test` with the `-race` argument to test for race conditions.
- Make sure your code is supported by all the go versions we support.

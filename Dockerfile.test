# Start from a Debian image with the latest version of Go installed
# and a workspace (GOPATH) configured at /go.
FROM golang

# Contact maintainer with any issues you encounter
MAINTAINER Richard Knop <risoknop@gmail.com>

# Set environment variables
ENV PATH /go/bin:$PATH

# Cd into the source code directory
WORKDIR /go/src/github.com/RichardKnop/machinery

# Copy the local package files to the container's workspace.
ADD . /go/src/github.com/RichardKnop/machinery

# Set GO111MODULE=on variable to activate module support
ENV GO111MODULE on

# Run integration tests as default command
CMD /go/src/github.com/RichardKnop/machinery/wait-for-it.sh rabbitmq:5672 -- make test-with-coverage

package tracing

import (
	"github.com/asynkron/protoactor-go/extensions"
	"go.opentelemetry.io/otel/trace"
)

var extensionID = extensions.NextExtensionID()

type TraceExtension struct {
	trace.TracerProvider
}

func (ext *TraceExtension) Tracer() trace.Tracer {
	return ext.TracerProvider.Tracer("protoactor")
}

func NewTraceExtension(
	provider trace.TracerProvider,
) *TraceExtension {
	return &TraceExtension{
		provider,
	}
}

func (ext *TraceExtension) Enabled() bool {
	return true
}

func (ext *TraceExtension) ExtensionID() extensions.ExtensionID {
	return extensionID
}

var _ extensions.Extension = &TraceExtension{}

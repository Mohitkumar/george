package discovery

import "go.uber.org/zap"

type LoggerHandler struct {
	logger *zap.Logger
}

func NewLoggerHandler() *LoggerHandler {
	return &LoggerHandler{
		logger: zap.L().Named("replicator"),
	}
}

func (h *LoggerHandler) Join(name, addr string) error {
	h.log("server joined cluster", addr)
	return nil
}

func (h *LoggerHandler) Leave(name string) error {
	h.log("server left cluster", name)
	return nil
}

func (r *LoggerHandler) log(msg, addr string) {
	r.logger.Info(
		msg,
		zap.String("addr", addr),
	)
}

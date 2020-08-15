package dmqp

//消息类型编号
const ClassConnection = 10
const ClassChannel = 20
const ClassExchange = 40
const ClassQueue = 50
const ClassBasic = 60
const ClassConfirm = 85
const MethodConnectionCloseOk = 51

const (
	FrameMethod        = 1
	FrameHeader        = 2
	FrameBody          = 3
	FrameHeartbeat     = 8
	FrameMinSize       = 4096
	FrameEnd           = 206
	replySuccess       = 200
	ContentTooLarge    = 311
	NoRoute            = 312
	NoConsumers        = 313
	ConnectionForced   = 320
	InvalidPath        = 402
	AccessRefused      = 403
	NotFound           = 404
	QueueExcluded     = 405
	PreconditionFailed = 406
	FrameError         = 501
	SyntaxError        = 502
	CommandInvalid     = 503
	ChannelError       = 504
	UnexpectedFrame    = 505
	ResourceError      = 506
	NotAllowed         = 530
	NotImplemented     = 540
	InternalError      = 541
)

var ConstantsNameMap = map[uint16]string{

	1: "FRAME_METHOD",

	2: "FRAME_HEADER",

	3: "FRAME_BODY",

	8: "FRAME_HEARTBEAT",

	4096: "FRAME_MIN_SIZE",

	206: "FRAME_END",

	200: "REPLY_SUCCESS",

	311: "CONTENT_TOO_LARGE",

	313: "NO_CONSUMERS",

	320: "CONNECTION_FORCED",

	402: "INVALID_PATH",

	403: "ACCESS_REFUSED",

	404: "NOT_FOUND",

	405: "RESOURCE_LOCKED",

	406: "PRECONDITION_FAILED",

	501: "FRAME_ERROR",

	502: "SYNTAX_ERROR",

	503: "COMMAND_INVALID",

	504: "CHANNEL_ERROR",

	505: "UNEXPECTED_FRAME",

	506: "RESOURCE_ERROR",

	530: "NOT_ALLOWED",

	540: "NOT_IMPLEMENTED",

	541: "INTERNAL_ERROR",
}

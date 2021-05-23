package csxegts

// ---------------------------------------------------------------------------------
// Common
// ---------------------------------------------------------------------------------

// Version of the EGTS header structure
const egtsHeaderProtocolVersion = byte(1) // 0x01

// Prefix of the EGTS header for current version
const egtsHeaderPrefix = "00"

// ---------------------------------------------------------------------------------
// Temporal and quantitative
// ---------------------------------------------------------------------------------

// Time to wait for a packet to be acknowledged (in seconds)
// TL_RESPONSE_TO in specification
const TlResponseTo = byte(5)

// The number of retries to send an unacknowledged packet
// TL_RESEND_ATTEMPTS in specification
const TlResendAttempts = byte(3)

// The time after which a repeated attempt will be made to establish a communication channel after it is broken (in seconds)
// TL_RECONNECT_TO in specification
const TlReconnectTo = byte(30)

// ---------------------------------------------------------------------------------
// Packet Priority
// ---------------------------------------------------------------------------------

// Highest routing priority of packet
const PacketPriorityHighest = "00"

// High routing priority of packet
const PacketPriorityHigh = "01"

// Normal routing priority of packet
const PacketPriorityNormal = "10"

// Low routing priority of packet
const PacketPriorityLow = "11"

// ---------------------------------------------------------------------------------
// Source Service On Device
// ---------------------------------------------------------------------------------

// Source Service on terminal
const SsodTerminal = "1"

// Source Service on telematic platform
const SsodPlatform = "0"

// ---------------------------------------------------------------------------------
// Recipient Service On Device
// ---------------------------------------------------------------------------------

// Recipient Service on terminal
const RsodTerminal = "1"

// Recipient Service on telematic platform
const RsodPlatform = "0"

// ---------------------------------------------------------------------------------
// Record Processing Priority
// ---------------------------------------------------------------------------------

// Highest priority of record processing
const RpPriorityHighest = "000"

// High priority of record processing
const RpPriorityHigh = "001"

// Above Normal priority of record processing
const RpPriorityAboveNormal = "010"

// Normal priority of record processing
const RpPriorityNormal = "011"

// Below normal priority of record processing
const RpPriorityBelowNormal = "100"

// Low priority of record processing
const RpPriorityLow = "101"

// Lowest priority of record processing
const RpPriorityLowest = "110"

// Idle priority of record processing
const RpPriorityIdle = "111"

// ---------------------------------------------------------------------------------
// Sources (events) that initiated sending telemetry
// Codes 17, 18, 26 reserved
// ---------------------------------------------------------------------------------

const SrcTimerEnabledIgnition = byte(0)
const SrcDriveDistance = byte(1)
const SrcExceedRotationAngel = byte(2)
const SrcResponse = byte(3)
const SrcChangeXState = byte(4)
const SrcTimerDisabledIgnition = byte(5)
const SrcPeripheralEquipOff = byte(6)
const SrcExceedSpeed = byte(7)
const SrcRestart = byte(8)
const SrcOverloadY = byte(9)
const SrcIntrusionSensorOn = byte(10)
const SrcBackupPowerOn = byte(11)
const SrcLowBackupPowerVoltage = byte(12)
const SrcAlertBtnPressed = byte(13)
const SrcOperatorVoiceRequest = byte(14)
const SrcEmergencyCall = byte(15)
const SrcExternalServiceData = byte(16)
const SrcBackupBatteryFailure = byte(19)
const SrcHardAcceleration = byte(20)
const SrcHardBraking = byte(21)
const SrcNavigationModuleFailure = byte(22)
const SrcAccidentSensorFailure = byte(23)
const SrcGSMAntennaFailure = byte(24)
const SrcNavigationAntennaFailure = byte(25)
const SrcReduceSpeed = byte(27)
const SrcDisabledIgnitioMove = byte(28)
const SrcTimerEmergencyTracking = byte(29)
const SrcBeginEndNavigation = byte(30)
const SrcUnstableNavigation = byte(31)
const SrcIPConnection = byte(32)
const SrcUnstableMobileNet = byte(33)
const SrcUnstableCommunication = byte(34)
const SrcModeChanging = byte(35)

// ---------------------------------------------------------------------------------
// Coordinate definition type
// ---------------------------------------------------------------------------------

const Fix2D = "0"
const Fix3D = "1"

// ---------------------------------------------------------------------------------
// Coordinate system
// ---------------------------------------------------------------------------------

// Coordinate system WGS-84
const CsWGS84 = "0"

// Russian state geocentric coordinate system
const CsEarthParams9002 = "1"

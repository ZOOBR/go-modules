package csxegts

// ---------------------------------------------------------------------------------
// Common
// ---------------------------------------------------------------------------------

// Version of the EGTS header structure
const egtsHeaderProtocolVersion = byte(1) // 0x01

// Prefix of the EGTS header for current version
const egtsHeaderPrefix = "00"

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

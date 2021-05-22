package csxegts

// ---------------------------------------------------------------------------------
// Common
// ---------------------------------------------------------------------------------

// Version of the EGTS header structure
const egtsHeaderProtocolVersion = byte(1) // 0x01

// Prefix of the EGTS header for current version
const egtsHeaderPrefix = "00"

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

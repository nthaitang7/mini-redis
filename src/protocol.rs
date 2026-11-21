use std::fmt;
use std::str;

/// One parsed RESP2 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<RespValue>),
}

impl RespValue {
    /// Serialize this value as RESP2 into an existing byte buffer.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            RespValue::Simple(value) => {
                out.push(b'+');
                out.extend_from_slice(value.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Error(value) => {
                out.push(b'-');
                out.extend_from_slice(value.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(value) => {
                out.push(b':');
                out.extend_from_slice(value.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Bulk(value) => {
                out.push(b'$');
                out.extend_from_slice(value.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(value);
                out.extend_from_slice(b"\r\n");
            }
            RespValue::Null => out.extend_from_slice(b"$-1\r\n"),
            RespValue::Array(values) => {
                out.push(b'*');
                out.extend_from_slice(values.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");

                for value in values {
                    value.encode(out);
                }
            }
        }
    }

    /// Serialize this value as RESP2 bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }
}

/// An input framing error that should close the offending connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError(pub String);

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ProtocolError {}

/// A command as a list of binary arguments.
pub type Command = Vec<Vec<u8>>;

/// Incrementally parses RESP arrays and inline commands from TCP chunks.
pub struct RespParser {
    buffer: Vec<u8>,
    max_request_bytes: usize,
    max_args: usize,
}

impl RespParser {
    pub fn new(max_request_bytes: usize, max_args: usize) -> Self {
        Self {
            buffer: Vec::new(),
            max_request_bytes,
            max_args,
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) -> Result<Vec<Command>, ProtocolError> {
        if !chunk.is_empty() {
            self.buffer.extend_from_slice(chunk);
        }

        let mut commands = Vec::new();
        let mut offset = 0;

        while offset < self.buffer.len() {
            match self.parse_at(offset)? {
                Some((args, next)) => {
                    if !args.is_empty() {
                        commands.push(args);
                    }
                    offset = next;
                }
                None => break,
            }
        }

        if offset > 0 {
            self.buffer.drain(..offset);
        }

        if self.buffer.len() > self.max_request_bytes {
            return Err(ProtocolError("request exceeds maximum size".to_string()));
        }

        Ok(commands)
    }

    fn parse_at(&self, offset: usize) -> Result<Option<(Command, usize)>, ProtocolError> {
        if self.buffer[offset] == b'*' {
            self.parse_array(offset)
        } else {
            self.parse_inline(offset)
        }
    }

    fn parse_array(&self, offset: usize) -> Result<Option<(Command, usize)>, ProtocolError> {
        let header_end = match find_crlf(&self.buffer, offset + 1) {
            Some(value) => value,
            None => {
                self.check_incomplete_size(offset)?;
                return Ok(None);
            }
        };

        let count = parse_unsigned(&self.buffer[offset + 1..header_end])
            .ok_or_else(|| ProtocolError("invalid array length".to_string()))?;

        if count > self.max_args {
            return Err(ProtocolError("invalid array length".to_string()));
        }

        if count == 0 {
            return Ok(Some((Vec::new(), header_end + 2)));
        }

        let mut args = Vec::with_capacity(count);
        let mut cursor = header_end + 2;

        for _ in 0..count {
            if cursor >= self.buffer.len() {
                self.check_incomplete_size(offset)?;
                return Ok(None);
            }

            if self.buffer[cursor] != b'$' {
                return Err(ProtocolError("expected a bulk string".to_string()));
            }

            let length_end = match find_crlf(&self.buffer, cursor + 1) {
                Some(value) => value,
                None => {
                    self.check_incomplete_size(offset)?;
                    return Ok(None);
                }
            };

            let length = parse_unsigned(&self.buffer[cursor + 1..length_end])
                .ok_or_else(|| ProtocolError("invalid bulk string length".to_string()))?;

            if length > self.max_request_bytes {
                return Err(ProtocolError("invalid bulk string length".to_string()));
            }

            let data_start = length_end + 2;
            let data_end = match data_start.checked_add(length) {
                Some(value) => value,
                None => return Err(ProtocolError("invalid bulk string length".to_string())),
            };

            let terminator_end = match data_end.checked_add(2) {
                Some(value) => value,
                None => return Err(ProtocolError("invalid bulk string length".to_string())),
            };

            if terminator_end > self.buffer.len() {
                self.check_incomplete_size(offset)?;
                return Ok(None);
            }

            if &self.buffer[data_end..terminator_end] != b"\r\n" {
                return Err(ProtocolError(
                    "bulk string is missing its terminator".to_string(),
                ));
            }

            args.push(self.buffer[data_start..data_end].to_vec());
            cursor = terminator_end;

            if cursor.saturating_sub(offset) > self.max_request_bytes {
                return Err(ProtocolError("request exceeds maximum size".to_string()));
            }
        }

        Ok(Some((args, cursor)))
    }

    fn parse_inline(&self, offset: usize) -> Result<Option<(Command, usize)>, ProtocolError> {
        let newline = match self.buffer[offset..].iter().position(|byte| *byte == b'\n') {
            Some(value) => offset + value,
            None => {
                self.check_incomplete_size(offset)?;
                return Ok(None);
            }
        };

        let mut line_end = newline;

        if line_end > offset && self.buffer[line_end - 1] == b'\r' {
            line_end -= 1;
        }

        if line_end.saturating_sub(offset) > self.max_request_bytes {
            return Err(ProtocolError("request exceeds maximum size".to_string()));
        }

        let args: Command = self.buffer[offset..line_end]
            .split(|byte| *byte == b' ' || *byte == b'\t')
            .filter(|part| !part.is_empty())
            .map(|part| part.to_vec())
            .collect();

        if args.len() > self.max_args {
            return Err(ProtocolError("too many arguments".to_string()));
        }

        Ok(Some((args, newline + 1)))
    }

    fn check_incomplete_size(&self, offset: usize) -> Result<(), ProtocolError> {
        if self.buffer.len().saturating_sub(offset) > self.max_request_bytes {
            Err(ProtocolError("request exceeds maximum size".to_string()))
        } else {
            Ok(())
        }
    }
}

fn find_crlf(buffer: &[u8], start: usize) -> Option<usize> {
    if start >= buffer.len() {
        return None;
    }

    buffer[start..]
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|value| start + value)
}

fn parse_unsigned(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() || !bytes.iter().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    str::from_utf8(bytes).ok()?.parse::<usize>().ok()
}

fn parse_signed(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }

    str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

/// Parse one complete RESP2 reply, returning None when more bytes are needed.
pub fn parse_reply(
    buffer: &[u8],
    offset: usize,
) -> Result<Option<(RespValue, usize)>, ProtocolError> {
    if offset >= buffer.len() {
        return Ok(None);
    }

    let kind = buffer[offset];
    let line_end = match find_crlf(buffer, offset + 1) {
        Some(value) => value,
        None => return Ok(None),
    };
    let header = &buffer[offset + 1..line_end];
    let next = line_end + 2;

    match kind {
        b'+' => Ok(Some((
            RespValue::Simple(String::from_utf8_lossy(header).into_owned()),
            next,
        ))),
        b'-' => Ok(Some((
            RespValue::Error(String::from_utf8_lossy(header).into_owned()),
            next,
        ))),
        b':' => {
            let value = parse_signed(header)
                .ok_or_else(|| ProtocolError("invalid integer reply".to_string()))?;
            Ok(Some((RespValue::Integer(value), next)))
        }
        b'$' => {
            let length = parse_signed(header)
                .ok_or_else(|| ProtocolError("invalid bulk string length".to_string()))?;

            if length == -1 {
                return Ok(Some((RespValue::Null, next)));
            }

            if length < 0 {
                return Err(ProtocolError("invalid bulk string length".to_string()));
            }

            let length = length as usize;
            let data_end = next
                .checked_add(length)
                .ok_or_else(|| ProtocolError("invalid bulk string length".to_string()))?;
            let terminator_end = data_end
                .checked_add(2)
                .ok_or_else(|| ProtocolError("invalid bulk string length".to_string()))?;

            if terminator_end > buffer.len() {
                return Ok(None);
            }

            if &buffer[data_end..terminator_end] != b"\r\n" {
                return Err(ProtocolError(
                    "bulk string is missing its terminator".to_string(),
                ));
            }

            Ok(Some((
                RespValue::Bulk(buffer[next..data_end].to_vec()),
                terminator_end,
            )))
        }
        b'*' => {
            let count = parse_signed(header)
                .ok_or_else(|| ProtocolError("invalid array length".to_string()))?;

            if count == -1 {
                return Ok(Some((RespValue::Null, next)));
            }

            if count < 0 {
                return Err(ProtocolError("invalid array length".to_string()));
            }

            let mut values = Vec::with_capacity(count as usize);
            let mut cursor = next;

            for _ in 0..count {
                let parsed = parse_reply(buffer, cursor)?;

                match parsed {
                    Some((value, next)) => {
                        values.push(value);
                        cursor = next;
                    }
                    None => return Ok(None),
                }
            }

            Ok(Some((RespValue::Array(values), cursor)))
        }
        _ => Err(ProtocolError("unknown reply type".to_string())),
    }
}

/// Buffers reply bytes and returns complete parsed values in order.
pub struct ReplyReader {
    buffer: Vec<u8>,
}

impl ReplyReader {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    pub fn read(&mut self) -> Result<Option<RespValue>, ProtocolError> {
        let parsed = parse_reply(&self.buffer, 0)?;

        match parsed {
            Some((value, next)) => {
                self.buffer.drain(..next);
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }
}

impl Default for ReplyReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode a command as a RESP array of bulk strings.
pub fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'*');
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");

    for arg in args {
        let value = arg.as_ref();
        out.push(b'$');
        out.extend_from_slice(value.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(args: &[Vec<u8>]) -> Vec<String> {
        args.iter()
            .map(|arg| String::from_utf8_lossy(arg).into_owned())
            .collect()
    }

    fn cmd(values: &[&str]) -> Vec<u8> {
        let args: Vec<&[u8]> = values.iter().map(|value| value.as_bytes()).collect();
        encode_command(&args)
    }

    #[test]
    fn parses_a_command_across_chunks() {
        let mut parser = RespParser::new(1024, 1024);
        assert!(parser.feed(b"*1\r\n$4\r\nPI").unwrap().is_empty());
        let commands = parser.feed(b"NG\r\n").unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(strings(&commands[0]), vec!["PING"]);
    }

    #[test]
    fn parses_pipelined_commands() {
        let mut parser = RespParser::new(1024, 1024);
        let mut input = Vec::new();
        input.extend(cmd(&["PING"]));
        input.extend(cmd(&["SET", "key", "value"]));
        input.extend(cmd(&["GET", "key"]));
        let commands = parser.feed(&input).unwrap();
        assert_eq!(commands.len(), 3);
        assert_eq!(strings(&commands[1]), vec!["SET", "key", "value"]);
    }

    #[test]
    fn preserves_binary_values() {
        let mut parser = RespParser::new(1024, 1024);
        let value: &[u8] = &[0, 1, 13, 10, 255];
        let input = encode_command(&[b"SET".as_slice(), b"binary".as_slice(), value]);
        let commands = parser.feed(&input).unwrap();
        assert_eq!(commands[0][2].as_slice(), value);
    }

    #[test]
    fn accepts_inline_commands() {
        let mut parser = RespParser::new(1024, 1024);
        let commands = parser
            .feed(b"PING\r\nSET greeting hello\nGET greeting\r\n")
            .unwrap();
        assert_eq!(commands.len(), 3);
        assert_eq!(strings(&commands[1]), vec!["SET", "greeting", "hello"]);
    }

    #[test]
    fn ignores_empty_requests() {
        let mut parser = RespParser::new(1024, 1024);
        let commands = parser.feed(b"\r\n\n*0\r\n*1\r\n$4\r\nPING\r\n").unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(strings(&commands[0]), vec!["PING"]);
    }

    #[test]
    fn rejects_malformed_input() {
        let mut array_parser = RespParser::new(1024, 1024);
        assert!(array_parser.feed(b"*x\r\n").is_err());

        let mut bulk_parser = RespParser::new(1024, 1024);
        assert!(bulk_parser.feed(b"*1\r\n$-1\r\n").is_err());
    }

    #[test]
    fn rejects_oversized_requests() {
        let mut parser = RespParser::new(32, 1024);
        let value = "x".repeat(100);
        assert!(parser.feed(&cmd(&["SET", "key", value.as_str()])).is_err());
    }

    #[test]
    fn encodes_replies() {
        assert_eq!(
            RespValue::Simple("OK".to_string()).to_bytes().as_slice(),
            b"+OK\r\n"
        );
        assert_eq!(
            RespValue::Error("ERR bad".to_string())
                .to_bytes()
                .as_slice(),
            b"-ERR bad\r\n"
        );
        assert_eq!(RespValue::Integer(42).to_bytes().as_slice(), b":42\r\n");
        assert_eq!(
            RespValue::Bulk(b"hello".to_vec()).to_bytes().as_slice(),
            b"$5\r\nhello\r\n"
        );
        assert_eq!(RespValue::Null.to_bytes().as_slice(), b"$-1\r\n");
    }

    #[test]
    fn parses_replies_incrementally() {
        let mut reader = ReplyReader::new();
        reader.push(b"*2\r\n$3\r\nfoo\r\n:");
        assert_eq!(reader.read().unwrap(), None);
        reader.push(b"7\r\n");
        assert_eq!(
            reader.read().unwrap(),
            Some(RespValue::Array(vec![
                RespValue::Bulk(b"foo".to_vec()),
                RespValue::Integer(7),
            ]))
        );
        assert_eq!(reader.buffered_bytes(), 0);
    }
}

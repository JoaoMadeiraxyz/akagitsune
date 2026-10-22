use std::fmt;

use subtle::ConstantTimeEq;

const MAX_TOKEN_LEN: usize = 128;

#[derive(Default)]
pub struct Tokens(Box<[Box<[u8]>]>);

#[derive(Debug, PartialEq, Eq)]
pub struct InvalidToken {
    position: usize,
}

impl fmt::Display for InvalidToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "token at position {} must be 1 to {MAX_TOKEN_LEN} bytes of the HTTP token charset",
            self.position
        )
    }
}

impl std::error::Error for InvalidToken {}

fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

pub fn parse_tokens(input: &str) -> Result<Tokens, InvalidToken> {
    let mut tokens = Vec::new();
    for (index, entry) in input.split(',').enumerate() {
        let entry = entry.trim().as_bytes();
        if entry.is_empty() {
            continue;
        }
        if entry.len() > MAX_TOKEN_LEN || !entry.iter().copied().all(is_token_byte) {
            return Err(InvalidToken {
                position: index + 1,
            });
        }
        tokens.push(entry.into());
    }
    Ok(Tokens(tokens.into()))
}

impl Tokens {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn admits(&self, presented: &[u8]) -> bool {
        let mut admitted = false;
        for token in &self.0 {
            if token.len() == presented.len() {
                admitted |= bool::from(token.ct_eq(presented));
            }
        }
        admitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_gives_an_empty_set() {
        assert!(parse_tokens("").unwrap().is_empty());
    }

    #[test]
    fn whitespace_and_empty_entries_give_an_empty_set() {
        assert!(parse_tokens("  , ,\t,").unwrap().is_empty());
    }

    #[test]
    fn entries_are_trimmed() {
        let tokens = parse_tokens(" a1 ,b2\t,, c3").unwrap();
        assert!(tokens.admits(b"a1"));
        assert!(tokens.admits(b"b2"));
        assert!(tokens.admits(b"c3"));
        assert!(!tokens.admits(b" a1 "));
    }

    #[test]
    fn full_charset_is_accepted() {
        let all = "!#$%&'*+-.^_`|~09azAZ";
        assert!(parse_tokens(all).unwrap().admits(all.as_bytes()));
    }

    #[test]
    fn inner_space_is_rejected() {
        assert!(parse_tokens("a b").is_err());
    }

    #[test]
    fn control_byte_is_rejected() {
        assert!(parse_tokens("a\u{1}b").is_err());
    }

    #[test]
    fn non_ascii_byte_is_rejected() {
        assert!(parse_tokens("caf\u{e9}").is_err());
    }

    #[test]
    fn separator_characters_are_rejected() {
        for bad in ["a\"b", "a(b", "a/b", "a:b", "a@b", "a[b", "a{b", "a\\b"] {
            assert!(parse_tokens(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn length_limit_is_128_bytes() {
        assert!(parse_tokens(&"a".repeat(128)).is_ok());
        assert!(parse_tokens(&"a".repeat(129)).is_err());
    }

    #[test]
    fn error_names_the_position_and_not_the_value() {
        let Err(error) = parse_tokens("good,,bad value") else {
            panic!("expected an error");
        };
        let text = error.to_string();
        assert!(text.contains("position 3"), "{text}");
        assert!(!text.contains("bad"), "{text}");
        assert!(!text.contains("value"), "{text}");
    }

    #[test]
    fn prefix_of_a_token_is_not_admitted() {
        let tokens = parse_tokens("secret-token").unwrap();
        assert!(tokens.admits(b"secret-token"));
        assert!(!tokens.admits(b"secret"));
        assert!(!tokens.admits(b"secret-token-and-more"));
        assert!(!tokens.admits(b""));
    }

    #[test]
    fn comparison_is_case_sensitive() {
        let tokens = parse_tokens("Abc").unwrap();
        assert!(tokens.admits(b"Abc"));
        assert!(!tokens.admits(b"abc"));
        assert!(!tokens.admits(b"ABC"));
    }

    #[test]
    fn every_configured_token_is_admitted() {
        let tokens = parse_tokens("one,two,three").unwrap();
        assert!(tokens.admits(b"one"));
        assert!(tokens.admits(b"two"));
        assert!(tokens.admits(b"three"));
        assert!(!tokens.admits(b"four"));
    }

    #[test]
    fn empty_set_admits_nothing() {
        assert!(!parse_tokens("").unwrap().admits(b"x"));
    }
}

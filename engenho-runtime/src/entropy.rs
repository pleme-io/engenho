//! OS entropy, and the one spelling of bytes as text: the admin bearer token
//! and the control plane's challenge ids are both random bytes in lowercase
//! hex.

/// `N` bytes from the OS's entropy source.
///
/// # Errors
///
/// The OS could not supply them.
pub fn random<const N: usize>() -> Result<[u8; N], getrandom::Error> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes)?;
    Ok(bytes)
}

/// `bytes` as lowercase hex, two characters a byte.
#[must_use]
pub fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from(DIGITS[usize::from(b >> 4)]));
        out.push(char::from(DIGITS[usize::from(b & 0xf)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_is_two_lowercase_digits_a_byte() {
        assert_eq!(lower_hex(&[0x00, 0x0f, 0xa5, 0xff]), "000fa5ff");
        assert_eq!(lower_hex(&[]), "");
    }

    #[test]
    fn two_draws_differ() {
        let (a, b) = (
            random::<16>().expect("entropy"),
            random::<16>().expect("entropy"),
        );
        assert_ne!(a, b);
    }
}

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PrincipalError {
    #[error("tenant_id must be 1..=128 characters of [A-Za-z0-9_-]")]
    Tenant,
    #[error("user_id must be 1..=128 characters of [A-Za-z0-9_-]")]
    User,
}

const MAX_PRINCIPAL_CHARS: usize = 128;

/// An authenticated caller whose identifiers are known to be safe as NATS key
/// segments and subject tokens.
///
/// Construction is the check. `tenant_id` becomes a *subject* token, where a
/// `.`, `*`, or `>` would corrupt the subject space, and both fields become KV
/// key segments. Building this at the auth boundary means no handler can be
/// reached with an unsafe principal — the guarantee is structural rather than a
/// rule every handler has to remember.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    tenant_id: String,
    user_id: String,
}

impl Principal {
    pub fn new(tenant_id: &str, user_id: &str) -> Result<Self, PrincipalError> {
        if !is_safe_segment(tenant_id) {
            return Err(PrincipalError::Tenant);
        }
        if !is_safe_segment(user_id) {
            return Err(PrincipalError::User);
        }
        Ok(Self {
            tenant_id: tenant_id.to_owned(),
            user_id: user_id.to_owned(),
        })
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

fn is_safe_segment(value: &str) -> bool {
    !value.is_empty()
        && value.chars().count() <= MAX_PRINCIPAL_CHARS
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_identifiers_are_accepted() {
        let principal = Principal::new("tenant-a", "user_1").expect("principal");
        assert_eq!(principal.tenant_id(), "tenant-a");
        assert_eq!(principal.user_id(), "user_1");
    }

    #[test]
    fn subject_and_key_metacharacters_are_rejected() {
        for tenant in ["a.b", "a*b", "a>b", "a/b", "", "a b"] {
            assert!(
                Principal::new(tenant, "user").is_err(),
                "tenant {tenant:?} should be rejected"
            );
        }
        for user in ["a.b", "a*b", "a>b", "a/b", ""] {
            assert!(
                Principal::new("tenant", user).is_err(),
                "user {user:?} should be rejected"
            );
        }
    }

    #[test]
    fn overlong_identifiers_are_rejected() {
        let long = "a".repeat(MAX_PRINCIPAL_CHARS + 1);
        assert_eq!(Principal::new(&long, "u"), Err(PrincipalError::Tenant));
        assert_eq!(Principal::new("t", &long), Err(PrincipalError::User));
    }
}
